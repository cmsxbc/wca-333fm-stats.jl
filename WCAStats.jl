module WCAStats

import Base
import StatsBase
import CSV
import DataFrames
import ZipFile


function load_wca(zip_path)
    d = Dict()
    z = ZipFile.Reader(zip_path)
    for name in ["Persons", "Results"]
        filename = "WCA_export_" * name * ".tsv"
        d[name] = CSV.read(read(z.files[findfirst(x->x.name==filename, z.files)]), DataFrames.DataFrame)
    end
    return d
end


function get_single_res_df(wca_dict, event_id)
    return filter(:value => x -> x != 0, stack(filter(:eventId => x -> x == event_id, wca_dict["Results"]), [:value1, :value2, :value3, :value4, :value5]))
end


function avg(xs)
    l = length(xs)
    if l > 2
        return (sum(xs) - sum(extrema(xs))) / l
    else
        return missing
    end
end


function mode_count(xs)
    modes = sort(StatsBase.modes(xs))
    return [(modes[1], count(==(modes[1]), xs))]
end


function calc_consecutive(xs, diffs)
    xs = sort(unique(xs))
    cstart = xs[1]
    ccount = 1
    cur_count = 1
    cur_start = xs[1]
    for i in 2:length(xs)
        if (xs[i] - xs[i-1]) in diffs
            cur_count += 1
        else
            if cur_count > ccount
                ccount = cur_count
                cstart = cur_start
            end
            cur_count = 1
            cur_start = xs[i]
        end
    end
    return [(ccount, cstart, cstart + ccount - 1)]
end


function stats_single_result(df, id_col, res_col)
    chances_df = DataFrames.combine(DataFrames.groupby(df, id_col), DataFrames.nrow => :chances)
    attempts_df = DataFrames.combine(DataFrames.groupby(filter(res_col => x -> x > -2, df), id_col), DataFrames.nrow => :attempts)
    solved_df = DataFrames.combine(
        DataFrames.groupby(filter(res_col => x -> x > 0, df), id_col),
        DataFrames.nrow => :solved_count,
        res_col => length ∘ DataFrames.unique => :solved_nunique,
        res_col => DataFrames.mean => :solved_mean,
        res_col => DataFrames.std => :solved_std,
        res_col => avg => :solved_avg,
        res_col => DataFrames.median => :solved_median,
        res_col => mode_count => [:solved_mode, :solved_mode_count],
        # res_col => (x -> [extrema(x)]) => [:solved_min, :solved_max],
        res_col => (x -> x |> extrema |> vcat ) => [:solved_min, :solved_max],
        res_col => Base.Fix2(calc_consecutive, 1) => [:solved_consecutive, :solved_consecutive_start, :solved_consecutive_end],
    )
    return DataFrames.leftjoin(DataFrames.leftjoin(chances_df, attempts_df, on=:personId), solved_df, on=:personId)
end


function __init__()
    wca_data = load_wca(ARGS[1])
    println("load data done")
    fm_single_res_df = get_single_res_df(wca_data, "333fm")
    df = stats_single_result(fm_single_res_df, :personId, :value)
    println(first(df, 10))
    println(filter(:personId => ==("2014WENW01"), df))
end

end # module WCAStats
