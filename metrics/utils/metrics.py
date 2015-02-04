'''
Created on Jan 15, 2015

@author: ehenneken
'''
import utils

def generate_metrics(**args):
    result = {}
    metrics_types = args.get('types',[])
    bibcodes = args.get('bibcodes')
    tori = args.get('tori',True)
    # Get basic data, used ubiquitously:
    # nonref: list of non-refereed papers (usually smaller than refereed)
    # normdict: dictionary with inverse author numbers (for normalized counts)
    nonreflist,skipped = utils.get_pub_data(bibcodes)
    # Remove skipped bibcodes from orignal list
    bibcodes = [p for p in bibcodes if p not in skipped]
    # Record the skipped bibcodes
    result['skipped bibcodes'] = skipped
    # Start calculating statistics
    if 'basic' in metrics_types:
        basic_stats, basic_stats_refereed = utils.get_stats(bibcodes,nonreflist)
        result['basic stats'] = basic_stats
        result['basic stats refereed'] = basic_stats_refereed
    if 'citations' in metrics_types:
        cite_stats, cite_stats_refereed = utils.get_citation_data(bibcodes,nonreflist)
        result['citation stats'] = cite_stats
        result['citation stats refereed'] = cite_stats_refereed
    if 'histograms' in metrics_types:
        hist_types = args.get('histograms')
        hists = utils.get_histograms(bibcodes,nonreflist,histograms=hist_types)
        result['histograms'] = hists
    if 'indicators' in metrics_types:
        ind, ind_refereed = utils.get_indicators(bibcodes,nonreflist,include_tori=tori)
        result['indicators'] = ind
        result['indicators refereed'] = ind_refereed
    if 'timeseries' in metrics_types:
        series = utils.get_time_series(bibcodes,include_tori=tori)
        result['time series'] = series
    return result
