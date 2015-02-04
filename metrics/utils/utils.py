'''
Created on Jan 15, 2015

@author: ehenneken
'''
from collections import defaultdict
from database import db, get_basic_stats, get_refereed, get_unique_citations, get_publication_histogram, get_usage_histogram, get_citation_histogram, get_self_citations, get_tori_index, get_h_index, get_g_index, get_iN_index
from math import sqrt
from flask.ext.sqlalchemy import SQLAlchemy
import sys
from datetime import datetime

def merge_tuples(list1,list2,subtract=False):
    merged = defaultdict(int)
    merged.update(list1)
    for key, value in list2:
        if subtract:
            merged[key] -= value
        else:
            merged[key] += value
    return merged.items()

def get_read10(l,year):
    now = datetime.now()
    if year==now.year:
        year -= 1
    bibs_10 = [p for p in l if (year - int(p[:4])) < 10]
    if len(bibs_10) == 0:
        return 0
    reads = get_usage_histogram(bibs_10, normalized=True)
    try:
        r10 = reads[year]
    except:
        r10 = 0
    return r10

def get_pub_data(bibcodes):
    # Make an overview of which publications are refereed
    refdict = get_refereed(bibcodes)
    nonrefereed = [r for r,s in refdict.items() if s == False]
    # Check which bibcodes were skipped
    # Which bibcodes were skipped?
    skipped_bibcodes = [p for p in bibcodes if p not in refdict]
    return nonrefereed,skipped_bibcodes

def get_stats(bibcodes,nonreflist):
    reflist = [p for p in bibcodes if p not in nonreflist]
    basic_stats = {}
    basic_stats_refereed = {}
    # Number of papers
    basic_stats['number of papers'] = len(bibcodes)
    basic_stats_refereed['number of papers'] = len(reflist)
    # Normalized number of papers
    basic_stats['normalized paper count'] = get_basic_stats(bibcodes,"papers")[2]
    basic_stats_refereed['normalized paper count'] = get_basic_stats(reflist,"papers")[2]
    # Get reads data for all papers
    basic_stats['median number of reads'], \
    basic_stats['average number of reads'], \
    basic_stats['total number of reads'], \
    basic_stats['normalized number of reads'] = get_basic_stats(bibcodes,"reads")
    # Get reads data for refereed papers
    basic_stats_refereed['median number of reads'], \
    basic_stats_refereed['average number of reads'], \
    basic_stats_refereed['total number of reads'], \
    basic_stats_refereed['normalized number of reads'] = get_basic_stats(reflist,"reads")
    # Get downloads data for all papers
    basic_stats['median number of downloads'], \
    basic_stats['average number of downloads'], \
    basic_stats['total number of downloads'], \
    basic_stats['normalized number of downloads'] = get_basic_stats(bibcodes,"downloads")
    # Get downloads data for refereed papers
    basic_stats_refereed['median number of downloads'], \
    basic_stats_refereed['average number of downloads'], \
    basic_stats_refereed['total number of downloads'], \
    basic_stats_refereed['normalized number of downloads'] = get_basic_stats(reflist,"downloads")
    # Send results back
    return basic_stats, basic_stats_refereed

def get_citation_data(bibcodes,nonreflist):
    cite_stats = {}
    cite_stats_refereed = {}
    # List of refereed bibcodes
    reflist = [p for p in bibcodes if p not in nonreflist]
    # Get the number of unique citing papers
    cite_stats['number of citing papers'] = get_unique_citations(bibcodes)
    cite_stats_refereed['Number of citing papers'] = get_unique_citations(reflist)
    # Get citation stats for all papers
    cite_stats['median number of citations'], \
    cite_stats['average number of citations'], \
    cite_stats['total number of citations'], \
    cite_stats['normalized number of citations'] = get_basic_stats(bibcodes,"citations")
    # Get refereed citation stats for all papers
    cite_stats['median number of refereed citations'], \
    cite_stats['average number of refereed citations'], \
    cite_stats['total number of refereed citations'], \
    cite_stats['normalized number of refereed citations'] = get_basic_stats(bibcodes,"citations", refereed=True)
    # Get citation stats for refereed papers
    cite_stats_refereed['median number of citations'], \
    cite_stats_refereed['average number of citations'], \
    cite_stats_refereed['total number of citations'], \
    cite_stats_refereed['normalized number of citations'] = get_basic_stats(reflist,"citations")
    # Get refereed citation stats for refereed papers
    cite_stats_refereed['median number of refereed citations'], \
    cite_stats_refereed['average number of refereed citations'], \
    cite_stats_refereed['total number of refereed citations'], \
    cite_stats_refereed['normalized number of refereed citations'] = get_basic_stats(reflist,"citations", refereed=True)
    # Get number of self-citations
    cite_stats['number of self-citations'] = get_self_citations(bibcodes)
    cite_stats_refereed['number of self-citations'] = get_self_citations(reflist)
    # Send results back
    return cite_stats, cite_stats_refereed

def get_histograms(bibcodes,nonreflist,histograms=['publications','reads','downloads','citations']):
    histodata = {}
    reflist = [p for p in bibcodes if p not in nonreflist]
    if 'publications' in histograms:
        pubhisto = {}
        # Publication histogram
        # All publications
        pubhisto['all publications'] = get_publication_histogram(bibcodes)
        # All publications normalized
        pubhisto['all publications normalized'] = get_publication_histogram(bibcodes, normalized=True)
        # Refereed publications
        pubhisto['refereed publications'] = get_publication_histogram(reflist)
        # Refereed publications normalized
        pubhisto['refereed publications normalized'] = get_publication_histogram(reflist, normalized=True)
        # Consolidate everything
        histodata['publications'] = pubhisto
    if 'reads' in histograms:
        readshisto = {}
        # Reads histograms
        # All reads
        readshisto['all reads'] = get_usage_histogram(bibcodes)
        # All reads, normalized by author numbers
        readshisto['all reads normalized'] = get_usage_histogram(bibcodes, normalized=True)
        # Reads for refereed papers
        readshisto['refereed reads'] = get_usage_histogram(reflist)
        # Refereed reads, normalized by author numbers
        readshisto['refereed reads normalized'] = get_usage_histogram(reflist, normalized=True)
        # Consolidate everything
        histodata['reads'] = readshisto
    if 'downloads' in histograms:
        downlhisto = {}
        # Downloads histograms
        # All downloads
        downlhisto['all downloads'] = get_usage_histogram(bibcodes, usage_type='downloads')
        # All reads, normalized by author numbers
        downlhisto['all downloads normalized'] = get_usage_histogram(bibcodes, usage_type='downloads', normalized=True)
        # Downloads for refereed papers
        downlhisto['refereed downloads'] = get_usage_histogram(reflist)
        # Refereed downloads, normalized by author numbers
        downlhisto['refereed downloads normalized'] = get_usage_histogram(reflist, usage_type='downloads', normalized=True)
        # Consolidate everything
        histodata['downloads'] = downlhisto
    if 'citations' in histograms:
        citationshisto = {}
        # Index conventions: 'r': refereeed, 'n': nonrefereed, 'a': all
        # Citation "directions" can be expressed as a 'matrix' with elements
        # c_ij, where c_ij is the number of citations of one type to the other. 
        # The matrix elements are not independent. For example:
        #      c_nr: "the number of nonrefereed papers citing refereed papers"
        # If you know c_rr and c_ar, then c_nr = c_ar - c_rr. Similarly:
        #      c_nn: "the number of nonrefereed papers citing nonrefereed papers"
        # If you know c_rn and c_an, then c_nn = c_an - c_rn.
        # Citation histogram for citations: refereed --> refereed
        # Record the citation years in the set 'years'
        years = set()
        cits_hist_rr = get_citation_histogram(reflist,refereed=True)
        minYear = [years.add(c[0]) for c in cits_hist_rr]
        # Citation histogram for citations: all --> refereed
        cits_hist_ar = get_citation_histogram(reflist)
        # Now we can calculate the histogram: non-refereed -> refereed
        # by subtracting the 'rr' histogram from the 'ar' histogram
        cits_hist_nr = merge_tuples(cits_hist_ar,cits_hist_rr,subtract=True)
        res = [years.add(c[0]) for c in cits_hist_nr]
        # Citation histogram for citations: refereed --> nonrefereed
        cits_hist_rn = get_citation_histogram(nonreflist,refereed=True)
        res = [years.add(c[0]) for c in cits_hist_rn]
        # Citation histogram for citations: all --> nonrefereed
        cits_hist_an = get_citation_histogram(nonreflist)
        # Now we can calculate the histogram: non-refereed -> non-refereed
        # by subtracting the 'rn' histogram from the 'an' histogram
        cits_hist_nn = merge_tuples(cits_hist_an,cits_hist_rn,subtract=True)
        res = [years.add(c[0]) for c in cits_hist_nn]
        citationshisto['refereed to refereed'] = dict([(year,dict(cits_hist_rr).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['nonrefereed to refereed'] = dict([(year,dict(cits_hist_nr).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['refereed to nonrefereed'] = dict([(year,dict(cits_hist_rn).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['nonrefereed to nonrefereed'] = dict([(year,dict(cits_hist_nn).get(year,0)) for year in range(min(years),max(years)+1)])
        # Now the normalized versions of everything
        cits_hist_rr = get_citation_histogram(reflist,refereed=True, normalized=True)
        minYear = [years.add(c[0]) for c in cits_hist_rr]
        # Citation histogram for citations: all --> refereed
        cits_hist_ar = get_citation_histogram(reflist, normalized=True)
        # Now we can calculate the histogram: non-refereed -> refereed
        # by subtracting the 'rr' histogram from the 'ar' histogram
        cits_hist_nr = merge_tuples(cits_hist_ar,cits_hist_rr,subtract=True)
        res = [years.add(c[0]) for c in cits_hist_nr]
        # Citation histogram for citations: refereed --> nonrefereed
        cits_hist_rn = get_citation_histogram(nonreflist,refereed=True, normalized=True)
        res = [years.add(c[0]) for c in cits_hist_rn]
        # Citation histogram for citations: all --> nonrefereed
        cits_hist_an = get_citation_histogram(nonreflist, normalized=True)
        # Now we can calculate the histogram: non-refereed -> non-refereed
        # by subtracting the 'rn' histogram from the 'an' histogram
        cits_hist_nn = merge_tuples(cits_hist_an,cits_hist_rn,subtract=True)
        res = [years.add(c[0]) for c in cits_hist_nn]
        citationshisto['refereed to refereed normalized'] = dict([(year,dict(cits_hist_rr).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['nonrefereed to refereed normalized'] = dict([(year,dict(cits_hist_nr).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['refereed to nonrefereed normalized'] = dict([(year,dict(cits_hist_rn).get(year,0)) for year in range(min(years),max(years)+1)])
        citationshisto['nonrefereed to nonrefereed normalized'] = dict([(year,dict(cits_hist_nn).get(year,0)) for year in range(min(years),max(years)+1)])
        histodata['citations'] = citationshisto
    return histodata

def get_indicators(bibcodes, nonreflist, include_tori=True):
    indicators = {}
    indicators_refereed = {}
    now = datetime.now()
    # Determine the bibcodes for refereed publications
    reflist = [p for p in bibcodes if p not in nonreflist]
    # Get the time (in years) since the first publication
    start_year = min([int(p[:4]) for p in bibcodes])
    span = now.year - start_year + 1
    # Get the time (in year) since the first refereed publication
    start_year = min([int(p[:4]) for p in reflist])
    span_refereed = now.year - start_year + 1
    # Calculate indcators
    # iN: number of publications with >= N citations
    indicators['i10'] = get_iN_index(bibcodes,10,now.year)
    indicators['i100'] = get_iN_index(bibcodes,100,now.year)
    indicators_refereed['i10'] = get_iN_index(reflist,10,now.year)
    indicators_refereed['i100'] = get_iN_index(reflist,100,now.year)
    # the h index
    indicators['h'] = get_h_index(bibcodes,now.year)
    indicators_refereed['h'] = get_h_index(reflist,now.year)
    # the m index: h index divided by the time span (years) of the citations
    indicators['m'] = float(indicators['h'])/float(span)
    indicators_refereed['m'] = float(indicators_refereed['h'])/float(span_refereed)
    # the g index
    indicators['g'] = get_g_index(bibcodes,now.year)
    indicators_refereed['g'] = get_g_index(reflist,now.year)
    # the READ10 index: current author-normalized readership rate
    # for papers published in last 10 years
    now = datetime.now()
    indicators['read10'] = get_read10(bibcodes,now.year)
    indicators_refereed['read10'] = get_read10(reflist,now.year)
    # the Tori index
    if include_tori:
        indicators['tori'] = get_tori_index(bibcodes,now.year)
        indicators['riq'] = int(1000.0*sqrt(float(indicators['tori']))/float(span))
        indicators_refereed['tori'] = get_tori_index(reflist,now.year)
        indicators_refereed['riq'] = int(1000.0*sqrt(float(indicators_refereed['tori']))/float(span_refereed))

    return indicators, indicators_refereed

def get_time_series(bibcodes, include_tori=True):
    timeseries = {}
    start_year = min([int(p[:4]) for p in bibcodes])
    end_year   = max([int(p[:4]) for p in bibcodes])
    i10_series = {}
    i100_series = {}
    h_series = {}
    g_series = {}
    read10_series = {}
    tori_series = {}
    for year in range(start_year, end_year+1):
        bibs = [p for p in bibcodes if int(p[:4]) <= year]
        i10_series[year]    = get_iN_index(bibs,10,year)
        i100_series[year]   = get_iN_index(bibs,100,year)
        h_series[year]      = get_h_index(bibs,year)
        g_series[year]      = get_g_index(bibs,year)
        read10_series[year] = get_read10(bibs,year)
        if include_tori:
            tori_series[year]   = get_tori_index(bibs,year)
    timeseries['i10']    = i10_series
    timeseries['i100']   = i100_series
    timeseries['h']      = h_series
    timeseries['g']      = g_series
    timeseries['read10'] = read10_series
    if include_tori:
        timeseries['tori']   = tori_series

    return timeseries
