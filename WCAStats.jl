module WCAStats

import Base
import StatsBase
import CSV
import DataFrames
import ZipFile
import RollingFunctions


function load_wca(zip_path)
    d = Dict()
    z = ZipFile.Reader(zip_path)
    for name in ["Persons", "Results"]
        filename = "WCA_export_" * name * ".tsv"
        d[name] = CSV.read(read(z.files[findfirst(x->x.name==filename, z.files)]), DataFrames.DataFrame)
    end
    return d
end


function get_event_result(wca_dict, event_id)
    return filter(:eventId => ==(event_id), wca_dict["Results"])
end


function get_single_res_df(wca_dict, event_id)
    return filter(:value => !=(0), stack(get_event_result(wca_dict, event_id), [:value1, :value2, :value3, :value4, :value5]))
end


nunique = length ∘ DataFrames.unique


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


function calc_rolling_mean(n, xs)
    if length(xs) < n
        return [(missing, missing)]
    else
        results = RollingFunctions.rollmean(xs, n)
        return [(results[end], minimum(results))]
    end
end


function calc_rolling_avg(n, xs)
    if length(xs) < n
        return [(missing, missing)]
    else
        results = RollingFunctions.rolling(avg, xs, n)
        return [(results[end], minimum(results))]
    end
end


function stats_single_result(df, id_col, res_col)
    chances_df = DataFrames.combine(DataFrames.groupby(df, id_col), DataFrames.nrow => :chances)
    attempts_df = DataFrames.combine(DataFrames.groupby(filter(res_col => x -> x > -2, df), id_col), DataFrames.nrow => :attempts)
    solved_df = DataFrames.combine(
        DataFrames.groupby(filter(res_col => x -> x > 0, df), id_col),
        DataFrames.nrow => :solved_count,
        res_col => nunique => :solved_nunique,
        res_col => DataFrames.mean => :solved_mean,
        res_col => DataFrames.std => :solved_std,
        res_col => avg => :solved_avg,
        res_col => DataFrames.median => :solved_median,
        res_col => mode_count => [:solved_mode, :solved_mode_count],
        # res_col => (x -> [extrema(x)]) => [:solved_min, :solved_max],
        res_col => (x -> x |> extrema |> vcat ) => [:solved_min, :solved_max],
        res_col => Base.Fix2(calc_consecutive, 1) => [:solved_consecutive, :solved_consecutive_start, :solved_consecutive_end],
        res_col => Base.Fix1(calc_rolling_mean, 12) => [:solved_mo12_last, :solved_mo12_best],
        res_col => Base.Fix1(calc_rolling_mean, 50) => [:solved_mo50_last, :solved_mo50_best],
        res_col => Base.Fix1(calc_rolling_mean, 100) => [:solved_mo100_last, :solved_mo100_best],
        res_col => Base.Fix1(calc_rolling_avg, 5) => [:solved_ao5_last, :solved_ao5_best],
        res_col => Base.Fix1(calc_rolling_avg, 12) => [:solved_ao12_last, :solved_ao12_best],
        res_col => Base.Fix1(calc_rolling_avg, 50) => [:solved_ao50_last, :solved_ao50_best],
        res_col => Base.Fix1(calc_rolling_avg, 100) => [:solved_ao100_last, :solved_ao100_best],
    )
    return DataFrames.leftjoin(DataFrames.leftjoin(chances_df, attempts_df, on=id_col), solved_df, on=id_col)
end


function stats_round_result(df, id_col)
    rdf = DataFrames.combine(
        DataFrames.groupby(df, id_col),
        :competitionId => nunique => :competitions,
        DataFrames.nrow => :rounds,
    )
    rdf = DataFrames.leftjoin(
        rdf,
        DataFrames.combine(
            DataFrames.groupby(filter(:best => >(0), df), id_col),
            :best => (x -> x |> extrema |> vcat ) => [:best, :best_max],
            DataFrames.nrow => :best_count,
            :best => nunique => :best_nunique,
            :best => DataFrames.mean => :best_mean,
            :best => DataFrames.std => :best_std,
            :best => avg => :best_avg,
            :best => DataFrames.median => :best_median,
            :best => mode_count => [:best_mode, :best_mode_count],
            :best => Base.Fix2(calc_consecutive, 1) => [:best_consecutive, :best_consecutive_start, :best_consecutive_end],
        ),
        on=id_col
    )
    rdf = DataFrames.leftjoin(
        rdf,
        DataFrames.combine(
            DataFrames.groupby(filter(:average => !=(0), df), id_col),
            DataFrames.nrow => :average_attempts,
        ),
        on=id_col
    )

    rdf = DataFrames.leftjoin(
        rdf,
        DataFrames.combine(
            DataFrames.groupby(
                DataFrames.transform(
                    filter(:average => >(0), df),
                    DataFrames.AsTable([:value1, :value2, :value3]) => DataFrames.ByRow(x -> [maximum(x), DataFrames.median(x)]) => [:wrost_in_average, :median_in_average]
                ),
                id_col,
            ),
            :average => (x -> x |> extrema |> vcat ) => [:average, :average_max],
            DataFrames.nrow => :average_count,
            :average => nunique => :average_nunique,
            :average => DataFrames.mean => :average_mean,
            :average => DataFrames.std => :average_std,
            :average => avg => :average_avg,
            :average => DataFrames.median => :average_median,
            :average => mode_count => [:average_mode, :average_mode_count],
            :average => Base.Fix2(calc_consecutive, 1) => [:average_consecutive, :average_consecutive_start, :average_consecutive_end],
            :wrost_in_average => (x -> x |> extrema |> vcat) => [:avg_item_3rd_min, :avg_item_3rd_max],
            :median_in_average => (x -> x |> extrema |> vcat) => [:avg_item_2nd_min, :avg_item_2nd_max],
        ),
        on=id_col
    )

    return rdf
end


function __init__()
    wca_data = load_wca(ARGS[1])
    println("load data done")
    fm_single_res_df = get_single_res_df(wca_data, "333fm")
    df = DataFrames.leftjoin(
        stats_round_result(get_event_result(wca_data, "333fm"), :personId),
        stats_single_result(fm_single_res_df, :personId, :value),
        on=:personId
    )
    df = DataFrames.rightjoin(
        DataFrames.rename(
            wca_data["Persons"][!, [:id, :name, :countryId, :gender]],
            :id => :personId, :name => :personName
        ),
        df,
        on=:personId
    )
    println(DataFrames.nrow(df))
    println(first(df, 10))
    println(filter(:personId => x -> x ∈ ["2014WENW01", "2008DONG06", "2012LIUY03"], df))
end

end # module WCAStats
