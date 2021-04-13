from pyecharts.charts import Line

def Charts(arr, start_time, end_time):
    # df = equity_curve
    # df_curve = df[df['datetime'] >= pd.to_datetime(start_time)]
    # df_curve = df_curve[df_curve['datetime'] <= pd.to_datetime(end_time)]
    # period_df = df_curve.resample(rule='D', on ='datetime').median()
    # period_df.reset_index(inplace=True)
    #
    # curve_x = period_df['datetime'].apply(
    #     lambda x: x.strftime('%Y-%m-%d %H:%M:%S')).values.tolist()
    # curve_y = period_df['equity_curve'].round(2).values.tolist()
    #
    # c = (
    #     Line().add_xaxis(curve_x).add_yaxis("收益曲线 %s" % symbol, curve_y,
    #                                         areastyle_opts=opts.AreaStyleOpts(opacity=0.5)).set_global_opts(
    #         title_opts=opts.TitleOpts(title='策略收益曲线图')).render("line_area_style.html")
    # )
    print("")