# Copyright 2011 Peter de Rivaz
# Trying to parse a wikipedia database file for fun
# Downloaded from http://dumps.wikimedia.org/
import re,time
from collections import defaultdict

base='c:/data/wikipedia/'
wiki=base+'frwikiquote-20110918'
pagelinks_sql=wiki+'-pagelinks.sql'
page_sql=wiki+'-page.sql'


def parse_links(pagelinks_sql,page_ids,action):
    """Parses file called pagelinks_sql to find all links, and runs action(from,to) on each valid link.

    page_ids should be a title->id dictionary calculated from the page.sql file via parse_page"""
    n=0
    miss=0
    fd=open(pagelinks_sql,'r')
    assert fd
    while 1:
        a=fd.readline()
        if len(a)==0: break
        if not a.startswith('INSERT'): continue
        print a[:100]
        it = re.finditer('\(([^,]*),([^,]*),([^,]*)\)',a)
        for match in it:
            page_namespace = int(match.group(2))
            if page_namespace!=0: continue # Namespace 0 contains the interesting pages
            page_from = int(match.group(1))
            page_to_title = match.group(3)[1:-1] # Strip off quotes
            try:
                page_to = page_ids[page_to_title]
                action(page_from,page_to)
            except KeyError:
                miss+=1
            n+=1
            if n%10**4==0: print n

    fd.close()
    print 'Found %d links (%d misses)' % (n,miss)


def parse_page(page_sql):
    """Parses file called page_sql to return the title to key and key to title dictionaries"""
    key2title={}
    title2key={}
    page_ids={}
    fd=open(page_sql,'r')
    assert fd
    n=0
    while 1:
        a=fd.readline()
        if len(a)==0: break
        if not a.startswith('INSERT'): continue
        print a[:100]
        it = re.finditer('\(([^,]*),([^,]*),([^,]*)[^\)]*\)',a)
        for match in it:
                page_namespace = int(match.group(2))
                if page_namespace!=0: continue # Namespace 0 contains the interesting pages
                page_from = int(match.group(1))
                page_to_title = match.group(3)[1:-1] # Strip off quotes
                title2key[page_to_title]=page_from
                key2title[page_from]=page_to_title
                n+=1
                if n%10**4==0: print n
    fd.close()
    return title2key,key2title

t0=time.time()
title2key,key2title=parse_page(page_sql)
print 'Found %d pages' % len(title2key)

D=defaultdict(int)
def action(pg_from,pg_to):
    D[pg_to]+=1  
parse_links(pagelinks_sql,title2key,action)
P=sorted(D.items(),key=lambda (k,c) :c,reverse=True)
for key,c in P: 
    print key2title[key],c
print time.time()-t0
#fd.close()
