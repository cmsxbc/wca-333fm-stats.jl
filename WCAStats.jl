module WCAStats

import Base
import ArgParse
import ZipFile
import Printf

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

struct Person
    wca_id::String
    sub_id::UInt16
    name::String
    country_id::String
    gender::String
end

struct Competition
    id::String
    year::Int32
end

struct Result333
    id::Int64
    pos::Int32
    best::Int32
    average::Int32
    comp_key::UInt32
    round_type_id::UInt8
    person_key::UInt32
    event_id::UInt16
end

struct Attempt
    result_id::Int64
    attempt_number::UInt8
    value::Int32
end

struct WcaData
    persons::Vector{Person}
    person_idx_by_wca_id::Dict{String, UInt32}
    competitions::Vector{Competition}
    comp_idx_by_id::Dict{String, UInt32}
    events::Vector{String}
    event_idx::Dict{String, UInt16}
    results::Vector{Result333}
    attempts_by_result::Dict{Int64, Vector{Attempt}}
end

function event_id(data::WcaData, name::String)
    get(data.event_idx, name, nothing)
end

function person_key(data::WcaData, wca_id::String)
    get(data.person_idx_by_wca_id, wca_id, nothing)
end

function event_years(data::WcaData, event_id::UInt16)
    seen = falses(length(data.competitions))
    lo = typemax(Int32)
    hi = typemin(Int32)
    for r in data.results
        r.event_id != event_id && continue
        cid = r.comp_key
        if !seen[cid]
            seen[cid] = true
            y = data.competitions[cid].year
            y < lo && (lo = y)
            y > hi && (hi = y)
        end
    end
    lo <= hi ? collect(Int32, lo:hi) : Int32[]
end

function person_event_years(data::WcaData, person_key::UInt32, event_id::UInt16)
    lo = typemax(Int32)
    hi = typemin(Int32)
    plo = typemax(Int32)
    for r in data.results
        r.event_id != event_id && continue
        y = data.competitions[r.comp_key].year
        lo = min(lo, y)
        hi = max(hi, y)
        r.person_key == person_key && (plo = min(plo, y))
    end
    plo != typemax(Int32) && lo <= hi ? collect(Int32, plo:hi) : Int32[]
end

# ---------------------------------------------------------------------------
# Fast byte-level TSV helpers
# ---------------------------------------------------------------------------

function fast_str(buf, l::Int, r::Int)::String
    len = r - l + 1
    len <= 0 && return ""
    @inbounds unsafe_string(pointer(buf, l), len)
end

function next_tab(buf, l::Int, line_stop::Int)::Union{Int,Nothing}
    r = findnext(==(UInt8('\t')), buf, l)
    (r === nothing || r > line_stop) ? nothing : r
end

function parse_int(::Type{T}, buf, l::Int, r::Int)::Union{T,Nothing} where T <: Integer
    r < l && return nothing
    i = l
    @inbounds neg = buf[i] == UInt8('-')
    if neg || buf[i] == UInt8('+')
        i += 1
        i > r && return nothing
    end
    n = zero(T)
    @inbounds while i <= r
        b = buf[i]
        if b < UInt8('0') || b > UInt8('9')
            return nothing
        end
        n = n * T(10) + T(b - UInt8('0'))
        i += 1
    end
    return neg ? -n : n
end

function header_col(hdr, name::String)::Union{Int,Nothing}
    l = 1
    col = 0
    len = length(hdr)
    @inbounds while l <= len
        r = findnext(==(UInt8('\t')), hdr, l)
        if r === nothing
            r = len + 1
        end
        if fast_str(hdr, l, r - 1) == name
            return col
        end
        l = r + 1
        col += 1
    end
    return nothing
end

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

function parse_persons(buf)
    p = findnext(==(UInt8('\n')), buf, 1)
    pos = p === nothing ? length(buf) + 1 : p + 1
    persons = Vector{Person}()
    person_idx = Dict{String, UInt32}()
    sizehint!(persons, 300_000)
    len = length(buf)
    @inbounds while pos <= len
        line_end = findnext(==(UInt8('\n')), buf, pos)
        line_end === nothing && (line_end = len + 1)
        line_stop = line_end - 1
        if line_stop >= pos && buf[line_stop] == UInt8('\r')
            line_stop -= 1
        end
        line_stop < pos && (pos = line_end + 1; continue)

        l = pos
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        name = fast_str(buf, l, r - 1); l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        gender = fast_str(buf, l, r - 1); l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        wca_id = fast_str(buf, l, r - 1); l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        sub_id = parse_int(UInt16, buf, l, r - 1)
        sub_id === nothing && (sub_id = UInt16(1))
        l = r + 1
        r = next_tab(buf, l, line_stop)
        if r === nothing
            country_id = fast_str(buf, l, line_stop)
        else
            country_id = fast_str(buf, l, r - 1)
        end

        if sub_id == 1
            person_idx[wca_id] = UInt32(length(persons) + 1)
        end
        push!(persons, Person(wca_id, sub_id, name, country_id, gender))
        pos = line_end + 1
    end
    persons, person_idx
end

function parse_competitions(buf)
    p = findnext(==(UInt8('\n')), buf, 1)
    pos = p === nothing ? length(buf) + 1 : p + 1
    hdr = @view buf[1:(p === nothing ? length(buf) : p - 1)]
    id_col = header_col(hdr, "id")
    year_col = header_col(hdr, "year")
    max_col = max(id_col, year_col)

    competitions = Vector{Competition}()
    comp_idx = Dict{String, UInt32}()
    sizehint!(competitions, 20_000)
    len = length(buf)

    @inbounds while pos <= len
        line_end = findnext(==(UInt8('\n')), buf, pos)
        line_end === nothing && (line_end = len + 1)
        line_stop = line_end - 1
        if line_stop >= pos && buf[line_stop] == UInt8('\r')
            line_stop -= 1
        end
        line_stop < pos && (pos = line_end + 1; continue)

        l = pos
        fields = Vector{UnitRange{Int}}()
        col = 0
        while l <= line_stop && col <= max_col
            r = findnext(==(UInt8('\t')), buf, l)
            if r === nothing || r > line_stop + 1
                push!(fields, l:line_stop)
                col += 1
                break
            else
                push!(fields, l:(r - 1))
                l = r + 1
                col += 1
            end
        end
        length(fields) <= max_col && (pos = line_end + 1; continue)

        id = fast_str(buf, fields[id_col + 1].start, fields[id_col + 1].stop)
        year = parse_int(Int32, buf, fields[year_col + 1].start, fields[year_col + 1].stop)
        year === nothing && (year = Int32(0))
        comp_idx[id] = UInt32(length(competitions) + 1)
        push!(competitions, Competition(id, year))
        pos = line_end + 1
    end
    competitions, comp_idx
end

function parse_results(buf, comp_idx, person_idx)
    p = findnext(==(UInt8('\n')), buf, 1)
    pos = p === nothing ? length(buf) + 1 : p + 1
    results = Vector{Result333}()
    sizehint!(results, 6_500_000)
    events = Vector{String}()
    event_idx = Dict{String, UInt16}()
    len = length(buf)

    @inbounds while pos <= len
        line_end = findnext(==(UInt8('\n')), buf, pos)
        line_end === nothing && (line_end = len + 1)
        line_stop = line_end - 1
        if line_stop >= pos && buf[line_stop] == UInt8('\r')
            line_stop -= 1
        end
        line_stop < pos && (pos = line_end + 1; continue)

        l = pos
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        id = parse_int(Int64, buf, l, r - 1); id === nothing && (pos = line_end + 1; continue)
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        pos_val = parse_int(Int32, buf, l, r - 1); pos_val === nothing && (pos_val = Int32(0))
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        best = parse_int(Int32, buf, l, r - 1); best === nothing && (best = Int32(0))
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        average = parse_int(Int32, buf, l, r - 1); average === nothing && (average = Int32(0))
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        comp_id = fast_str(buf, l, r - 1); l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        round_type = buf[l]
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        ev = fast_str(buf, l, r - 1); l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        l = r + 1  # skip person_name
        r = next_tab(buf, l, line_stop)
        if r === nothing
            person_id = fast_str(buf, l, line_stop)
        else
            person_id = fast_str(buf, l, r - 1)
        end

        comp_key = get(comp_idx, comp_id, nothing)
        comp_key === nothing && (pos = line_end + 1; continue)
        pk = get(person_idx, person_id, nothing)
        pk === nothing && (pos = line_end + 1; continue)

        eid = get(event_idx, ev, nothing)
        if eid === nothing
            eid = UInt16(length(events) + 1)
            push!(events, ev)
            event_idx[ev] = eid
        end

        push!(results, Result333(id, pos_val, best, average, comp_key, round_type, pk, eid))
        pos = line_end + 1
    end
    sort!(results, by=r->(r.person_key, r.id))
    results, events, event_idx
end

function parse_attempts(buf, wanted)
    p = findnext(==(UInt8('\n')), buf, 1)
    pos = p === nothing ? length(buf) + 1 : p + 1
    attempts_by_result = Dict{Int64, Vector{Attempt}}()
    sizehint!(attempts_by_result, length(wanted))
    len = length(buf)
    @inbounds while pos <= len
        line_end = findnext(==(UInt8('\n')), buf, pos)
        line_end === nothing && (line_end = len + 1)
        line_stop = line_end - 1
        if line_stop >= pos && buf[line_stop] == UInt8('\r')
            line_stop -= 1
        end
        line_stop < pos && (pos = line_end + 1; continue)

        l = pos
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        value = parse_int(Int32, buf, l, r - 1); value === nothing && (pos = line_end + 1; continue)
        l = r + 1
        r = next_tab(buf, l, line_stop); r === nothing && (pos = line_end + 1; continue)
        attempt_number = parse_int(UInt8, buf, l, r - 1); attempt_number === nothing && (pos = line_end + 1; continue)
        l = r + 1
        result_id = parse_int(Int64, buf, l, line_stop); result_id === nothing && (pos = line_end + 1; continue)

        if result_id in wanted
            vec = get!(Vector{Attempt}, attempts_by_result, result_id)
            push!(vec, Attempt(result_id, attempt_number, value))
        end
        pos = line_end + 1
    end
    for v in values(attempts_by_result)
        sort!(v, by=a->a.attempt_number)
    end
    attempts_by_result
end

function load_wca(zip_path)
    occursin("WCA_export_v2_", zip_path) || return nothing
    z = ZipFile.Reader(zip_path)
    local persons_buf, comps_buf, results_buf, attempts_buf
    for f in z.files
        if f.name == "WCA_export_persons.tsv"
            persons_buf = read(f)
        elseif f.name == "WCA_export_competitions.tsv"
            comps_buf = read(f)
        elseif f.name == "WCA_export_results.tsv"
            results_buf = read(f)
        elseif f.name == "WCA_export_result_attempts.tsv"
            attempts_buf = read(f)
        end
    end
    close(z)
    persons_buf === nothing && return nothing
    comps_buf === nothing && return nothing
    results_buf === nothing && return nothing
    attempts_buf === nothing && return nothing

    persons, person_idx = parse_persons(persons_buf)
    competitions, comp_idx = parse_competitions(comps_buf)
    results, events, event_idx = parse_results(results_buf, comp_idx, person_idx)
    wanted = Set(r.id for r in results)
    attempts_by_result = parse_attempts(attempts_buf, wanted)
    WcaData(persons, person_idx, competitions, comp_idx, events, event_idx, results, attempts_by_result)
end


# ---------------------------------------------------------------------------
# Statistical helpers (matching Rust / Julia semantics)
# ---------------------------------------------------------------------------

function trim_avg_i(xs::Vector{Int64})::Union{Float64,Nothing}
    n = length(xs)
    n <= 2 && return nothing
    s = Int128(0)
    mn = typemax(Int64)
    mx = typemin(Int64)
    for v in xs
        s += v
        v < mn && (mn = v)
        v > mx && (mx = v)
    end
    Float64(s - mn - mx) / (n - 2)
end

function trim_avg_f!(buf::Vector{Float64})::Union{Float64,Nothing}
    n = length(buf)
    n <= 2 && return nothing
    sort!(buf)
    pairwise_sum_f(@view buf[2:n-1]) / (n - 2)
end

function mean_i(xs::Vector{Int64})
    s = Int128(0)
    for v in xs
        s += v
    end
    Float64(s) / length(xs)
end

function pairwise_sum_f(xs::AbstractVector{Float64})::Float64
    n = length(xs)
    if n <= 16
        s = 0.0
        for v in xs
            s += v
        end
        return s
    end
    m = n ÷ 2
    pairwise_sum_f(@view xs[1:m]) + pairwise_sum_f(@view xs[m+1:n])
end

function sum_f(xs::Vector{Float64})
    pairwise_sum_f(xs)
end

function mean_f(xs::Vector{Float64})
    s = 0.0
    for v in xs
        s += v
    end
    s / length(xs)
end

function std_i(xs::Vector{Int64})
    n = length(xs)
    n < 2 && return NaN
    m = mean_i(xs)
    s = 0.0
    for v in xs
        d = Float64(v) - m
        s += d * d
    end
    sqrt(s / (n - 1))
end

function std_f(xs::Vector{Float64})
    n = length(xs)
    n < 2 && return NaN
    m = mean_f(xs)
    s = 0.0
    for v in xs
        d = v - m
        s += d * d
    end
    sqrt(s / (n - 1))
end

function median_f_from_i!(buf::Vector{Int64})::Float64
    sort!(buf)
    n = length(buf)
    isodd(n) ? Float64(buf[(n + 1) ÷ 2]) : (buf[n ÷ 2] + buf[n ÷ 2 + 1]) / 2.0
end

function median_i!(buf::Vector{Int64})::Int64
    sort!(buf)
    n = length(buf)
    isodd(n) ? buf[(n + 1) ÷ 2] : (buf[n ÷ 2] + buf[n ÷ 2 + 1]) ÷ 2
end

function mode_count_i!(buf::Vector{Int64})::Tuple{Int64,Int64}
    sort!(buf)
    best_val = buf[1]
    best_cnt = 1
    cur_val = buf[1]
    cur_cnt = 1
    for i in 2:length(buf)
        if buf[i] == cur_val
            cur_cnt += 1
        else
            if cur_cnt > best_cnt
                best_cnt = cur_cnt
                best_val = cur_val
            end
            cur_val = buf[i]
            cur_cnt = 1
        end
    end
    if cur_cnt > best_cnt
        best_cnt = cur_cnt
        best_val = cur_val
    end
    best_val, best_cnt
end

function calc_consecutive!(buf::Vector{Int64}, diffs::Vector{Int64})::Tuple{Int64,Int64,Int64}
    sort!(buf)
    # dedup in-place
    j = 2
    for i in 2:length(buf)
        if buf[i] != buf[j - 1]
            buf[j] = buf[i]
            j += 1
        end
    end
    ulen = j - 1
    ccount = 1
    cstart = buf[1]
    cend = buf[1]
    cur_count = 1
    cur_start = buf[1]
    for i in 2:ulen
        d = buf[i] - buf[i - 1]
        if d in diffs
            cur_count += 1
        else
            if cur_count > ccount
                ccount = cur_count
                cstart = cur_start
                cend = buf[i - 1]
            end
            cur_count = 1
            cur_start = buf[i]
        end
    end
    if cur_count > ccount
        ccount = cur_count
        cstart = cur_start
        cend = buf[ulen]
    end
    ccount, cstart, cend
end

function rolling_mean(xs::Vector{Int64}, n::Int)::Union{Tuple{Float64,Float64},Nothing}
    m = length(xs)
    m < n && return nothing
    denom = Float64(n)
    s = Int128(0)
    for i in 1:n
        s += xs[i]
    end
    first_val = Float64(s) / denom
    min_val = first_val
    last_val = first_val
    for i in (n + 1):m
        s += xs[i] - xs[i - n]
        last_val = Float64(s) / denom
        last_val < min_val && (min_val = last_val)
    end
    last_val, min_val
end

function rolling_trim_avg(xs::Vector{Int64}, n::Int)::Union{Tuple{Float64,Float64},Nothing}
    m = length(xs)
    (m < n || n <= 2) && return nothing
    denom = Float64(n - 2)
    last_val = 0.0
    min_val = Inf
    window = Vector{Int64}(undef, n)
    for i in n:m
        for j in 1:n
            window[j] = xs[i - n + j]
        end
        s = Int128(0)
        mn = typemax(Int64)
        mx = typemin(Int64)
        for v in window
            s += v
            v < mn && (mn = v)
            v > mx && (mx = v)
        end
        avg = Float64(s - mn - mx) / denom
        last_val = avg
        avg < min_val && (min_val = avg)
    end
    last_val, min_val
end

function extrema_i(xs::Vector{Int64})::Tuple{Int64,Int64}
    mn = xs[1]
    mx = xs[1]
    for i in 2:length(xs)
        v = xs[i]
        v < mn && (mn = v)
        v > mx && (mx = v)
    end
    mn, mx
end

function extrema_f(xs::Vector{Float64})::Tuple{Float64,Float64}
    mn = xs[1]
    mx = xs[1]
    for i in 2:length(xs)
        v = xs[i]
        v < mn && (mn = v)
        v > mx && (mx = v)
    end
    mn, mx
end

function copy_to_scratch!(dest::Vector{T}, src::AbstractVector{T})::Vector{T} where T
    resize!(dest, length(src))
    copyto!(dest, src)
    dest
end

# ---------------------------------------------------------------------------
# Column schema (69 value columns)
# ---------------------------------------------------------------------------

const COLS = [
    ("competitions",              :Int,   :Desc),
    ("rounds",                    :Int,   :Desc),
    ("best",                      :Int,   :Asc),
    ("best_max",                  :Int,   :Asc),
    ("best_count",                :Int,   :Desc),
    ("best_nunique",              :Int,   :Desc),
    ("best_mean",                 :Float, :Asc),
    ("best_std",                  :Float, :Asc),
    ("best_avg",                  :Float, :Asc),
    ("best_median",               :Float, :Asc),
    ("best_mode",                 :Int,   :Asc),
    ("best_mode_count",           :Int,   :Desc),
    ("best_consecutive",          :Int,   :Desc),
    ("best_consecutive_start",    :Int,   :Asc),
    ("best_consecutive_end",      :Int,   :Asc),
    ("average_attempts",          :Int,   :Desc),
    ("average",                   :Float, :Asc),
    ("average_max",               :Float, :Asc),
    ("average_count",             :Int,   :Desc),
    ("average_nunique",           :Int,   :Desc),
    ("average_mean",              :Float, :Asc),
    ("average_std",               :Float, :Asc),
    ("average_avg",               :Float, :Asc),
    ("average_median",            :Float, :Asc),
    ("average_mode",              :Float, :Asc),
    ("average_mode_count",        :Int,   :Desc),
    ("average_consecutive",       :Int,   :Desc),
    ("average_consecutive_start", :Float, :Asc),
    ("average_consecutive_end",   :Float, :Asc),
    ("gold",                      :Int,   :Desc),
    ("silver",                    :Int,   :Desc),
    ("bronze",                    :Int,   :Desc),
    ("chances",                   :Int,   :Desc),
    ("attempts",                  :Int,   :Desc),
    ("solved_count",              :Int,   :Desc),
    ("solved_nunique",            :Int,   :Desc),
    ("solved_mean",               :Float, :Asc),
    ("solved_std",                :Float, :Asc),
    ("solved_avg",                :Float, :Asc),
    ("solved_median",             :Float, :Asc),
    ("solved_mode",               :Int,   :Asc),
    ("solved_mode_count",         :Int,   :Desc),
    ("solved_min",                :Int,   :Asc),
    ("solved_max",                :Int,   :Asc),
    ("solved_consecutive",        :Int,   :Desc),
    ("solved_consecutive_start",  :Int,   :Asc),
    ("solved_consecutive_end",    :Int,   :Asc),
    ("solved_mo3_last",           :Float, :Asc),
    ("solved_mo3_best",           :Float, :Asc),
    ("solved_mo5_last",           :Float, :Asc),
    ("solved_mo5_best",           :Float, :Asc),
    ("solved_mo12_last",          :Float, :Asc),
    ("solved_mo12_best",          :Float, :Asc),
    ("solved_mo50_last",          :Float, :Asc),
    ("solved_mo50_best",          :Float, :Asc),
    ("solved_mo100_last",         :Float, :Asc),
    ("solved_mo100_best",         :Float, :Asc),
    ("solved_ao5_last",           :Float, :Asc),
    ("solved_ao5_best",           :Float, :Asc),
    ("solved_ao12_last",          :Float, :Asc),
    ("solved_ao12_best",          :Float, :Asc),
    ("solved_ao50_last",          :Float, :Asc),
    ("solved_ao50_best",          :Float, :Asc),
    ("solved_ao100_last",         :Float, :Asc),
    ("solved_ao100_best",         :Float, :Asc),
    ("avg_item_3rd_min",          :Int,   :Asc),
    ("avg_item_3rd_max",          :Int,   :Asc),
    ("avg_item_2nd_min",          :Int,   :Asc),
    ("avg_item_2nd_max",          :Int,   :Asc),
]

const COL_IDX = Dict(name => i for (i, (name, kind, dir)) in enumerate(COLS))

const DESC_ORDER = [
    "competitions", "rounds", "chances", "attempts",
    "solved_count", "solved_nunique", "solved_mode_count", "solved_consecutive",
    "best_count", "best_nunique", "best_mode_count", "best_consecutive",
    "average_attempts", "average_count", "average_nunique", "average_mode_count", "average_consecutive",
    "gold", "silver", "bronze"
]

const RANK_COL_ORDER = let
    order = Int[]
    for (i, (_, _, dir)) in enumerate(COLS)
        dir == :Asc && push!(order, i)
    end
    for name in DESC_ORDER
        push!(order, COL_IDX[name])
    end
    order
end

struct ColIdx
    competitions::Int
    rounds::Int
    best::Int
    best_max::Int
    best_count::Int
    best_nunique::Int
    best_mean::Int
    best_std::Int
    best_avg::Int
    best_median::Int
    best_mode::Int
    best_mode_count::Int
    best_consecutive::Int
    best_consecutive_start::Int
    best_consecutive_end::Int
    average_attempts::Int
    average::Int
    average_max::Int
    average_count::Int
    average_nunique::Int
    average_mean::Int
    average_std::Int
    average_avg::Int
    average_median::Int
    average_mode::Int
    average_mode_count::Int
    average_consecutive::Int
    average_consecutive_start::Int
    average_consecutive_end::Int
    gold::Int
    silver::Int
    bronze::Int
    chances::Int
    attempts::Int
    solved_count::Int
    solved_nunique::Int
    solved_mean::Int
    solved_std::Int
    solved_avg::Int
    solved_median::Int
    solved_mode::Int
    solved_mode_count::Int
    solved_min::Int
    solved_max::Int
    solved_consecutive::Int
    solved_consecutive_start::Int
    solved_consecutive_end::Int
    solved_mo::Vector{Tuple{Int,Int,Int}}
    solved_ao::Vector{Tuple{Int,Int,Int}}
    avg_item_3rd_min::Int
    avg_item_3rd_max::Int
    avg_item_2nd_min::Int
    avg_item_2nd_max::Int
end

function ColIdx()
    ColIdx(
        COL_IDX["competitions"],
        COL_IDX["rounds"],
        COL_IDX["best"],
        COL_IDX["best_max"],
        COL_IDX["best_count"],
        COL_IDX["best_nunique"],
        COL_IDX["best_mean"],
        COL_IDX["best_std"],
        COL_IDX["best_avg"],
        COL_IDX["best_median"],
        COL_IDX["best_mode"],
        COL_IDX["best_mode_count"],
        COL_IDX["best_consecutive"],
        COL_IDX["best_consecutive_start"],
        COL_IDX["best_consecutive_end"],
        COL_IDX["average_attempts"],
        COL_IDX["average"],
        COL_IDX["average_max"],
        COL_IDX["average_count"],
        COL_IDX["average_nunique"],
        COL_IDX["average_mean"],
        COL_IDX["average_std"],
        COL_IDX["average_avg"],
        COL_IDX["average_median"],
        COL_IDX["average_mode"],
        COL_IDX["average_mode_count"],
        COL_IDX["average_consecutive"],
        COL_IDX["average_consecutive_start"],
        COL_IDX["average_consecutive_end"],
        COL_IDX["gold"],
        COL_IDX["silver"],
        COL_IDX["bronze"],
        COL_IDX["chances"],
        COL_IDX["attempts"],
        COL_IDX["solved_count"],
        COL_IDX["solved_nunique"],
        COL_IDX["solved_mean"],
        COL_IDX["solved_std"],
        COL_IDX["solved_avg"],
        COL_IDX["solved_median"],
        COL_IDX["solved_mode"],
        COL_IDX["solved_mode_count"],
        COL_IDX["solved_min"],
        COL_IDX["solved_max"],
        COL_IDX["solved_consecutive"],
        COL_IDX["solved_consecutive_start"],
        COL_IDX["solved_consecutive_end"],
        [(3,  COL_IDX["solved_mo3_last"],  COL_IDX["solved_mo3_best"]),
         (5,  COL_IDX["solved_mo5_last"],  COL_IDX["solved_mo5_best"]),
         (12, COL_IDX["solved_mo12_last"], COL_IDX["solved_mo12_best"]),
         (50, COL_IDX["solved_mo50_last"], COL_IDX["solved_mo50_best"]),
         (100,COL_IDX["solved_mo100_last"],COL_IDX["solved_mo100_best"])],
        [(5,  COL_IDX["solved_ao5_last"],  COL_IDX["solved_ao5_best"]),
         (12, COL_IDX["solved_ao12_last"], COL_IDX["solved_ao12_best"]),
         (50, COL_IDX["solved_ao50_last"], COL_IDX["solved_ao50_best"]),
         (100,COL_IDX["solved_ao100_last"],COL_IDX["solved_ao100_best"])],
        COL_IDX["avg_item_3rd_min"],
        COL_IDX["avg_item_3rd_max"],
        COL_IDX["avg_item_2nd_min"],
        COL_IDX["avg_item_2nd_max"],
    )
end

const CI = ColIdx()

# ---------------------------------------------------------------------------
# Cells, Rows, Frames, Scratch
# ---------------------------------------------------------------------------

const Cell = Union{Missing, Int64, Float64}

mutable struct Row
    person_key::UInt32
    person_id::String
    person_name::String
    country_id::String
    gender::String
    vals::Vector{Cell}
    ranks::Vector{Cell}
    nrs::Vector{Cell}
    year::Union{Missing,Int64}
    category::Union{Missing,String}
end

struct Frame
    rows::Vector{Row}
    year_filter::Symbol
    year::Int32
end

function empty_row(person_key::UInt32)
    n = length(COLS)
    Row(person_key, "", "", "", "",
        fill(missing, n), fill(missing, n), fill(missing, n), missing, missing)
end

function row_for_person(frame::Frame, person_key::UInt32)::Union{Row,Nothing}
    for r in frame.rows
        r.person_key == person_key && return r
    end
    nothing
end

mutable struct Scratch
    bests::Vector{Int64}
    avgs_i::Vector{Int64}
    avgs_real::Vector{Float64}
    avgs_sorted::Vector{Float64}
    uniq::Vector{Int64}
    solved::Vector{Int64}
    worsts::Vector{Int64}
    medians::Vector{Int64}
    att_vs::Vector{Int64}
    comp_keys::Vector{UInt32}
    tmp_i64::Vector{Int64}
    tmp_f64::Vector{Float64}
    single_values::Vector{Union{Missing,Int32}}
    Scratch() = new(Int64[], Int64[], Float64[], Float64[], Int64[], Int64[], Int64[], Int64[], Int64[], UInt32[], Int64[], Float64[], Union{Missing,Int32}[])
end

function clear!(sc::Scratch)
    empty!(sc.bests)
    empty!(sc.avgs_i)
    empty!(sc.avgs_real)
    empty!(sc.avgs_sorted)
    empty!(sc.uniq)
    empty!(sc.solved)
    empty!(sc.worsts)
    empty!(sc.medians)
    empty!(sc.att_vs)
    empty!(sc.comp_keys)
    empty!(sc.tmp_i64)
    empty!(sc.tmp_f64)
    empty!(sc.single_values)
end

# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------

function nan_lt_asc(a::Float64, b::Float64)
    isnan(b) ? !isnan(a) : (isnan(a) ? false : a < b)
end

function nan_lt_desc(a::Float64, b::Float64)
    isnan(a) ? !isnan(b) : (isnan(b) ? false : a > b)
end

function competerank_col(rows::Vector{Row}, ci::Int, dir::Symbol, subset=nothing)
    n = subset === nothing ? length(rows) : length(subset)
    out = Vector{Cell}(missing, n)
    present = Vector{Tuple{Int,Float64}}()
    sizehint!(present, n)
    get_row = subset === nothing ? (k -> rows[k]) : (k -> rows[subset[k]])
    for k in 1:n
        v = get_row(k).vals[ci]
        v === missing && continue
        f = v isa Int64 ? Float64(v) : v::Float64
        push!(present, (k, f))
    end
    isempty(present) && return out
    if dir == :Desc
        sort!(present, lt=(x, y)->nan_lt_desc(x[2], y[2]))
    else
        sort!(present, lt=(x, y)->nan_lt_asc(x[2], y[2]))
    end
    rank = 1
    prev = nothing
    for (pos, (k, v)) in enumerate(present)
        eq = prev !== nothing && prev == v
        if !eq
            rank = pos
        end
        out[k] = Int64(rank)
        prev = v
    end
    out
end

function compute_ranks!(rows::Vector{Row})
    for ci in 1:length(COLS)
        dir = COLS[ci][3]
        ranks = competerank_col(rows, ci, dir, nothing)
        for i in 1:length(rows)
            rows[i].ranks[ci] = ranks[i]
        end
    end
    by_country = Dict{String, Vector{Int}}()
    for (i, r) in enumerate(rows)
        if haskey(by_country, r.country_id)
            push!(by_country[r.country_id], i)
        else
            by_country[r.country_id] = [i]
        end
    end
    for ci in 1:length(COLS)
        dir = COLS[ci][3]
        for idxs in values(by_country)
            ranks = competerank_col(rows, ci, dir, idxs)
            for j in 1:length(idxs)
                rows[idxs[j]].nrs[ci] = ranks[j]
            end
        end
    end
end


# ---------------------------------------------------------------------------
# Calculation
# ---------------------------------------------------------------------------

function compute_row!(row::Row, data::WcaData, person_key::UInt32, idxs::AbstractVector{Int}, sc::Scratch)
    clear!(sc)
    ci = CI

    # --- round-level stats ---
    for i in idxs
        push!(sc.comp_keys, data.results[i].comp_key)
    end
    sort!(sc.comp_keys)
    unique!(sc.comp_keys)
    row.vals[ci.competitions] = Int64(length(sc.comp_keys))
    row.vals[ci.rounds] = Int64(length(idxs))

    # best stats on best > 0
    for i in idxs
        r = data.results[i]
        r.best > 0 && push!(sc.bests, Int64(r.best))
    end
    if !isempty(sc.bests)
        bests = sc.bests
        mn, mx = extrema_i(bests)
        row.vals[ci.best] = mn
        row.vals[ci.best_max] = mx
        row.vals[ci.best_count] = Int64(length(bests))
        copy_to_scratch!(sc.uniq, bests)
        sort!(sc.uniq)
        unique!(sc.uniq)
        row.vals[ci.best_nunique] = Int64(length(sc.uniq))
        row.vals[ci.best_mean] = mean_i(bests)
        row.vals[ci.best_std] = std_i(bests)
        v = trim_avg_i(bests)
        v !== nothing && (row.vals[ci.best_avg] = v)
        row.vals[ci.best_median] = median_f_from_i!(copy_to_scratch!(sc.tmp_i64, bests))
        mode, mc = mode_count_i!(copy_to_scratch!(sc.tmp_i64, bests))
        row.vals[ci.best_mode] = mode
        row.vals[ci.best_mode_count] = mc
        cc, cs, ce = calc_consecutive!(copy_to_scratch!(sc.tmp_i64, bests), Int64[1])
        row.vals[ci.best_consecutive] = cc
        row.vals[ci.best_consecutive_start] = cs
        row.vals[ci.best_consecutive_end] = ce
    end

    # average_attempts on average != 0
    avg_attempts = count(i -> data.results[i].average != 0, idxs)
    avg_attempts > 0 && (row.vals[ci.average_attempts] = Int64(avg_attempts))

    # average stats on average > 0
    for i in idxs
        r = data.results[i]
        r.average > 0 && push!(sc.avgs_i, Int64(r.average))
    end
    if !isempty(sc.avgs_i)
        for v in sc.avgs_i
            push!(sc.avgs_real, v / 100.0)
        end
        avgs_i = sc.avgs_i
        avgs_real = sc.avgs_real
        mn, mx = extrema_f(avgs_real)
        row.vals[ci.average] = mn
        row.vals[ci.average_max] = mx
        row.vals[ci.average_count] = Int64(length(avgs_real))
        copy_to_scratch!(sc.uniq, avgs_i)
        sort!(sc.uniq)
        unique!(sc.uniq)
        row.vals[ci.average_nunique] = Int64(length(sc.uniq))
        row.vals[ci.average_mean] = mean_f(avgs_real)
        row.vals[ci.average_std] = std_f(avgs_real)
        v = trim_avg_f!(copy_to_scratch!(sc.tmp_f64, avgs_real))
        v !== nothing && (row.vals[ci.average_avg] = v)
        copy_to_scratch!(sc.avgs_sorted, avgs_real)
        sort!(sc.avgs_sorted)
        n = length(sc.avgs_sorted)
        med = isodd(n) ? sc.avgs_sorted[(n + 1) ÷ 2] : (sc.avgs_sorted[n ÷ 2] + sc.avgs_sorted[n ÷ 2 + 1]) / 2.0
        row.vals[ci.average_median] = med
        mode_i, mc = mode_count_i!(copy_to_scratch!(sc.tmp_i64, avgs_i))
        row.vals[ci.average_mode] = mode_i / 100.0
        row.vals[ci.average_mode_count] = mc
        cc, cs, ce = calc_consecutive!(copy_to_scratch!(sc.tmp_i64, avgs_i), Int64[33, 34])
        row.vals[ci.average_consecutive] = cc
        row.vals[ci.average_consecutive_start] = cs / 100.0
        row.vals[ci.average_consecutive_end] = ce / 100.0
    end

    # medals: final rounds (f or c) with best > 0
    g, s, b = 0, 0, 0
    any_final_best = false
    for i in idxs
        r = data.results[i]
        if r.best > 0 && (r.round_type_id == UInt8('f') || r.round_type_id == UInt8('c'))
            any_final_best = true
            if r.pos == 1
                g += 1
            elseif r.pos == 2
                s += 1
            elseif r.pos == 3
                b += 1
            end
        end
    end
    if any_final_best
        row.vals[ci.gold] = Int64(g)
        row.vals[ci.silver] = Int64(s)
        row.vals[ci.bronze] = Int64(b)
    end

    # --- single attempts ---
    for i in idxs
        r = data.results[i]
        atts = get(data.attempts_by_result, r.id, nothing)
        if atts !== nothing && !isempty(atts)
            for a in atts
                push!(sc.single_values, a.value)
            end
        else
            push!(sc.single_values, missing)
        end
    end

    row.vals[ci.chances] = Int64(length(sc.single_values))
    attempts_count = count(v -> v !== missing && v > -2, sc.single_values)
    row.vals[ci.attempts] = Int64(attempts_count)

    for v in sc.single_values
        v !== missing && v > 0 && push!(sc.solved, Int64(v))
    end

    if !isempty(sc.solved)
        solved = sc.solved
        row.vals[ci.solved_count] = Int64(length(solved))
        copy_to_scratch!(sc.uniq, solved)
        sort!(sc.uniq)
        unique!(sc.uniq)
        row.vals[ci.solved_nunique] = Int64(length(sc.uniq))
        row.vals[ci.solved_mean] = mean_i(solved)
        row.vals[ci.solved_std] = std_i(solved)
        v = trim_avg_i(solved)
        v !== nothing && (row.vals[ci.solved_avg] = v)
        row.vals[ci.solved_median] = median_f_from_i!(copy_to_scratch!(sc.tmp_i64, solved))
        mode, mc = mode_count_i!(copy_to_scratch!(sc.tmp_i64, solved))
        row.vals[ci.solved_mode] = mode
        row.vals[ci.solved_mode_count] = mc
        mn, mx = extrema_i(solved)
        row.vals[ci.solved_min] = mn
        row.vals[ci.solved_max] = mx
        cc, cs, ce = calc_consecutive!(copy_to_scratch!(sc.tmp_i64, solved), Int64[1])
        row.vals[ci.solved_consecutive] = cc
        row.vals[ci.solved_consecutive_start] = cs
        row.vals[ci.solved_consecutive_end] = ce
        for (n, last_i, best_i) in ci.solved_mo
            res = rolling_mean(solved, n)
            if res !== nothing
                row.vals[last_i] = res[1]
                row.vals[best_i] = res[2]
            end
        end
        for (n, last_i, best_i) in ci.solved_ao
            res = rolling_trim_avg(solved, n)
            if res !== nothing
                row.vals[last_i] = res[1]
                row.vals[best_i] = res[2]
            end
        end
    end

    # --- avg_item_3rd / 2nd ---
    for i in idxs
        r = data.results[i]
        r.average <= 0 && continue
        atts = get(data.attempts_by_result, r.id, nothing)
        (atts === nothing || isempty(atts)) && continue
        empty!(sc.att_vs)
        for a in atts
            push!(sc.att_vs, Int64(a.value))
        end
        push!(sc.worsts, maximum(sc.att_vs))
        push!(sc.medians, median_i!(copy_to_scratch!(sc.tmp_i64, sc.att_vs)))
    end
    if !isempty(sc.worsts)
        mn, mx = extrema_i(sc.worsts)
        row.vals[ci.avg_item_3rd_min] = mn
        row.vals[ci.avg_item_3rd_max] = mx
        mn, mx = extrema_i(sc.medians)
        row.vals[ci.avg_item_2nd_min] = mn
        row.vals[ci.avg_item_2nd_max] = mx
    end
end

function calc(data::WcaData, event_id::UInt16, year_filter::Symbol, year::Int32)
    kept = Vector{Int}(undef, 0)
    sizehint!(kept, length(data.results) ÷ 16)
    for (i, r) in enumerate(data.results)
        r.event_id != event_id && continue
        y = data.competitions[r.comp_key].year
        if year_filter == :eq
            y == year || continue
        else
            y <= year || continue
        end
        push!(kept, i)
    end

    person_slices = Vector{Tuple{UInt32,Int,Int}}()
    if !isempty(kept)
        start = 1
        cur_pk = data.results[kept[1]].person_key
        for i in 2:length(kept)
            pk = data.results[kept[i]].person_key
            if pk != cur_pk
                push!(person_slices, (cur_pk, start, i - 1))
                start = i
                cur_pk = pk
            end
        end
        push!(person_slices, (cur_pk, start, length(kept)))
    end

    person_order = [pk for (pk, _, _) in person_slices]
    sort!(person_order, by=pk -> begin
        p = data.persons[pk]
        haskey(data.person_idx_by_wca_id, p.wca_id) ? (0, p.wca_id) : (1, p.wca_id)
    end)

    pk_to_slice = Dict{UInt32, Tuple{Int,Int}}()
    for (pk, s, e) in person_slices
        pk_to_slice[pk] = (s, e)
    end

    n_cols = length(COLS)
    rows = Vector{Row}()
    sizehint!(rows, length(person_order))
    scratch = Scratch()
    for pk in person_order
        p = data.persons[pk]
        row = Row(pk, p.wca_id, p.name, p.country_id, p.gender,
                  fill(missing, n_cols), fill(missing, n_cols), fill(missing, n_cols),
                  missing, missing)
        (s, e) = pk_to_slice[pk]
        compute_row!(row, data, pk, @view(kept[s:e]), scratch)
        push!(rows, row)
    end

    compute_ranks!(rows)
    Frame(rows, year_filter, year)
end


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

function write_cell(io, v::Cell)
    if v === missing
        # empty
    elseif v isa Int64
        print(io, v::Int64)
    else
        f = v::Float64
        if isnan(f)
            write(io, "NaN")
        elseif isinf(f)
            write(io, f > 0 ? "Inf" : "-Inf")
        else
            print(io, f)
        end
    end
end

function write_str_csv(io, s::String)
    if occursin(',', s) || occursin('"', s) || occursin('\n', s)
        write(io, '"')
        for ch in s
            if ch == '"'
                write(io, "\"\"")
            else
                print(io, ch)
            end
        end
        write(io, '"')
    else
        write(io, s)
    end
end

function write_csv(path::String, frame::Frame)
    open(path, "w") do io
        write(io, "personId,personName,countryId,gender")
        for (name, _, _) in COLS
            write(io, ',', name)
        end
        for i in RANK_COL_ORDER
            write(io, ',', COLS[i][1], "_rank")
        end
        for i in RANK_COL_ORDER
            write(io, ',', COLS[i][1], "_nr")
        end
        write(io, '\n')
        for row in frame.rows
            write_str_csv(io, row.person_id)
            write(io, ',')
            write_str_csv(io, row.person_name)
            write(io, ',')
            write_str_csv(io, row.country_id)
            write(io, ',')
            write_str_csv(io, row.gender)
            for v in row.vals
                write(io, ',')
                write_cell(io, v)
            end
            for i in RANK_COL_ORDER
                write(io, ',')
                write_cell(io, row.ranks[i])
            end
            for i in RANK_COL_ORDER
                write(io, ',')
                write_cell(io, row.nrs[i])
            end
            write(io, '\n')
        end
    end
end

function write_summary_csv(path::String, header::Vector{String}, rows::Vector{Row})
    open(path, "w") do io
        for (i, h) in enumerate(header)
            i > 1 && write(io, ',')
            write(io, h)
        end
        write(io, '\n')
        for row in rows
            write_str_csv(io, row.person_id)
            write(io, ',')
            write_str_csv(io, row.person_name)
            write(io, ',')
            write_str_csv(io, row.country_id)
            write(io, ',')
            write_str_csv(io, row.gender)
            for v in row.vals
                write(io, ',')
                write_cell(io, v)
            end
            for i in RANK_COL_ORDER
                write(io, ',')
                write_cell(io, row.ranks[i])
            end
            for i in RANK_COL_ORDER
                write(io, ',')
                write_cell(io, row.nrs[i])
            end
            write(io, ',')
            row.year === missing || print(io, row.year::Int64)
            write(io, ',')
            row.category === missing || write_str_csv(io, row.category::String)
            write(io, '\n')
        end
    end
end

function print_some_persons(frame::Frame, person_ids::Vector{String})
    id_set = Set(person_ids)
    rows = Row[r for r in frame.rows if r.person_id in id_set]
    isempty(rows) && return
    name_lens = [max(length(r.person_name), 1) for r in rows]
    col_names = String["personId", "personName", "countryId", "gender"]
    for (name, _, _) in COLS
        push!(col_names, name)
    end
    for i in RANK_COL_ORDER
        push!(col_names, COLS[i][1] * "_rank")
    end
    for i in RANK_COL_ORDER
        push!(col_names, COLS[i][1] * "_nr")
    end
    col_name_len = maximum(length.(col_names))

    for (label, getter) in [("personId", r->r.person_id), ("personName", r->r.person_name),
                            ("countryId", r->r.country_id), ("gender", r->r.gender)]
        Printf.@printf "%*s" col_name_len label
        for (i, r) in enumerate(rows)
            Printf.@printf "    %*s" name_lens[i] getter(r)
        end
        println()
    end

    function print_numeric_row(name, cells)
        Printf.@printf "%*s" col_name_len name
        for (i, cell) in enumerate(cells)
            if cell === missing
                Printf.@printf "    %*s" name_lens[i] ""
            elseif cell isa Int64
                Printf.@printf "    %*d" name_lens[i] cell
            else
                Printf.@printf "    %*.*f" name_lens[i] 2 cell
            end
        end
        println()
    end

    for (i, (name, _, _)) in enumerate(COLS)
        print_numeric_row(name, [r.vals[i] for r in rows])
    end
    for i in RANK_COL_ORDER
        print_numeric_row(WCAStats.COLS[i][1] * "_rank", [r.ranks[i] for r in rows])
    end
    for i in RANK_COL_ORDER
        print_numeric_row(WCAStats.COLS[i][1] * "_nr", [r.nrs[i] for r in rows])
    end
end

function print_topk(frame::Frame, col::String, k::Int, country::Union{String,Nothing})
    ci = COL_IDX[col]
    use_nr = country !== nothing
    filtered = use_nr ? Row[r for r in frame.rows if r.country_id == country] : frame.rows
    withrank = Tuple{Int64,Row}[]
    for r in filtered
        cell = use_nr ? r.nrs[ci] : r.ranks[ci]
        cell === missing && continue
        v = cell::Int64
        v <= k && push!(withrank, (v, r))
    end
    sort!(withrank, by=x->x[1])
    nr_name = col * "_nr"
    rank_name = col * "_rank"
    Printf.@printf "%20s %20s %20s %10s %10s\n" "personName" "countryId" col nr_name rank_name
    for (_, r) in withrank
        vstr = if r.vals[ci] === missing
            ""
        elseif r.vals[ci] isa Int64
            string(r.vals[ci]::Int64)
        else
            string(r.vals[ci]::Float64)
        end
        nrs = r.nrs[ci] === missing ? "" : string(r.nrs[ci]::Int64)
        rks = r.ranks[ci] === missing ? "" : string(r.ranks[ci]::Int64)
        Printf.@printf "%20s %20s %20s %10s %10s\n" r.person_name r.country_id vstr nrs rks
    end
end

# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

function cell_sub(a::Cell, b::Cell, kind::Symbol)::Cell
    a === missing || b === missing ? missing : begin
        x = a isa Int64 ? Float64(a) : a::Float64
        y = b isa Int64 ? Float64(b) : b::Float64
        d = x - y
        kind == :Int ? trunc(Int64, d) : d
    end
end

function row_delta(row::Row, other::Row)::Row
    n = length(COLS)
    new_vals = Vector{Cell}(undef, n)
    new_ranks = Vector{Cell}(undef, n)
    new_nrs = Vector{Cell}(undef, n)
    for i in 1:n
        kind = COLS[i][2]
        new_vals[i] = cell_sub(row.vals[i], other.vals[i], kind)
        new_ranks[i] = cell_sub(row.ranks[i], other.ranks[i], :Int)
        new_nrs[i] = cell_sub(row.nrs[i], other.nrs[i], :Int)
    end
    cat = if row.category === missing
        missing
    else
        c = row.category::String
        if endswith(c, "-year")
            c[1:end-5] * "-year-detla"
        else
            c * "-detla"
        end
    end
    Row(row.person_key, row.person_id, row.person_name, row.country_id, row.gender,
        new_vals, new_ranks, new_nrs, row.year, cat)
end

function header_for_summary()::Vector{String}
    h = String["personId", "personName", "countryId", "gender"]
    for (name, _, _) in COLS
        push!(h, name)
    end
    for i in RANK_COL_ORDER
        push!(h, COLS[i][1] * "_rank")
    end
    for i in RANK_COL_ORDER
        push!(h, COLS[i][1] * "_nr")
    end
    push!(h, "year")
    push!(h, "category")
    h
end

# ---------------------------------------------------------------------------
# CLI and main driver
# ---------------------------------------------------------------------------

function process_data(parsed_args)
    src = parsed_args["source"]
    wca_data = load_wca(src)
    wca_data === nothing && (println("cannot load data"); return)
    println("Load Data done")

    result_dir = "results"
    isdir(result_dir) || mkdir(result_dir)

    ev_id = event_id(wca_data, "333fm")
    ev_id === nothing && (println("333fm event missing"); return)

    is_summary = parsed_args["%COMMAND%"] === "summary"
    summary_person_id = if is_summary
        pid = person_key(wca_data, parsed_args["summary"]["id"])
        pid === nothing && (println("unknown person id"); return)
        pid
    else
        nothing
    end

    years = if is_summary
        person_event_years(wca_data, summary_person_id, ev_id)
    else
        event_years(wca_data, ev_id)
    end
    sort!(years)
    y = parsed_args["year"]
    if y !== nothing && !is_summary
        filter!(==(Int32(y)), years)
    end

    summary_rows = Row[]
    last_rows = Dict{String, Row}()
    summary_header = is_summary ? header_for_summary() : nothing

    n_years = length(years)
    for (yi, year) in enumerate(years)
        println("dealing ", year, " ...")
        for (category, filter_sym) in [("in", :eq), ("to", :le)]
            df = calc(wca_data, ev_id, filter_sym, year)
            filename = joinpath(result_dir, "results.$(category)$(year).csv")
            write_csv(filename, df)
            println("saved: ", filename)

            if is_summary
                pid = summary_person_id
                row = row_for_person(df, pid)
                if row === nothing
                    row = empty_row(pid)
                    p = wca_data.persons[pid]
                    row.person_id = p.wca_id
                    row.person_name = p.name
                    row.country_id = p.country_id
                    row.gender = p.gender
                end
                row.year = Int64(year)
                row.category = "$(category)-year"
                summary_header === nothing && (summary_header = header_for_summary())
                push!(summary_rows, deepcopy(row))
                if haskey(last_rows, category)
                    delta = row_delta(row, last_rows[category])
                    delta.year = Int64(year)
                    delta.category = "$(category)-year-detla"
                    push!(summary_rows, delta)
                end
                last_rows[category] = row
            end

            is_last_year = yi == n_years
            if is_last_year && category == "to"
                cmd = parsed_args["%COMMAND%"]
                if cmd === "topk"
                    print_topk(df, parsed_args["topk"]["col"], parsed_args["topk"]["k"],
                               parsed_args["topk"]["country"])
                elseif cmd === "person"
                    ids = parsed_args["person"]["ids"]
                    print_some_persons(df, ids isa Vector{String} ? ids : String.(ids))
                end
            end
        end
    end

    if is_summary
        pid_str = parsed_args["summary"]["id"]
        path = joinpath(result_dir, "$(pid_str).csv")
        write_summary_csv(path, summary_header, summary_rows)
    end
end

function __init__()
    s = ArgParse.ArgParseSettings(commands_are_required=false)
    ArgParse.@add_arg_table! s begin
        "source"
        required = true
        "person", "P"
        help = "print some persons"
        action = :command
        "topk", "K"
        help = "print topk"
        action = :command
        "summary", "S"
        help = "summary one person"
        action = :command
        "--year"
        help = "only stats special year"
        arg_type = Int
    end
    ArgParse.@add_arg_table! s["person"] begin
        "ids"
        nargs = '*'
        action = "store_arg"
    end
    ArgParse.@add_arg_table! s["topk"] begin
        "col"
        "--k"
        default = 10
        arg_type = Int
        "--country"
        default = nothing
    end
    ArgParse.@add_arg_table! s["summary"] begin
        "id"
        required = true
    end
    parsed_args = ArgParse.parse_args(s)
    process_data(parsed_args)
end

end # module WCAStats
