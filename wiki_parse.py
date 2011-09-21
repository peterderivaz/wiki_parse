# Copyright 2011 Peter de Rivaz
# Trying to parse a wikipedia database file for fun
# Downloaded from http://dumps.wikimedia.org/
#
# Took 2400 seconds to find 284 million links in english wikipedia
# Reduces 19Gig to 1.2Gig of arrays (uses 2gig of RAM in the process, this could change to be streaming)

import re,time,gzip,cPickle,os
from collections import defaultdict
from array import array

compressed=True
base='c:/data/wikipedia/'
#wiki=base+'frwikiquote-20110918'
wiki=base+'enwiki-20110901'
pagelinks_sql=wiki+'-pagelinks.sql'
page_sql=wiki+'-page.sql'
outname=wiki+'.pql'
outname2=wiki+'links.raw'
myopen=open
if compressed:
    pagelinks_sql+='.gz'
    page_sql+='.gz'
    myopen=gzip.open

    

def parse_links(pagelinks_sql,page_ids,action):
    """Parses file called pagelinks_sql to find all links, and runs action(from,to) on each valid link.

    page_ids should be a title->id dictionary calculated from the page.sql file via parse_page"""
    n=0
    miss=0
    fd=myopen(pagelinks_sql,'rb')
    assert fd
    while 1:
        a=fd.readline()
        if len(a)==0: break
        if not a.startswith('INSERT'): continue
        print a[:100]
        it = re.finditer('\(([^,]*),([^,]*),([^,]*)\)',a)
        for match in it:
            try:
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
                if n%10**5==0: print n
            except ValueError:
                pass

    fd.close()
    print 'Found %d links (%d misses)' % (n,miss)


def parse_page(page_sql):
    """Parses file called page_sql to return the title to key and key to title dictionaries"""
    key2title={}
    title2key={}
    page_ids={}
    fd=myopen(page_sql,'r')
    assert fd
    n=0
    while 1:
        a=fd.readline()
        if len(a)==0: break
        if not a.startswith('INSERT'): continue
        print a[:100]
        it = re.finditer('\(([^,]*),([^,]*),([^,]*)[^\)]*\)',a)
        for match in it:
            try:
                page_namespace = int(match.group(2))
                if page_namespace!=0: continue # Namespace 0 contains the interesting pages
                page_from = int(match.group(1))
                page_to_title = match.group(3)[1:-1] # Strip off quotes
                title2key[page_to_title]=page_from
                key2title[page_from]=page_to_title
                n+=1
                if n%10**5==0: print n
            except ValueError:
                pass
                
    fd.close()
    return title2key,key2title

def extract_links():
    """Parses the page and pagelinks sql.gz files to produce a .pql and .raw file.
    The .pql contains a pickled dictionary of key->title
    The .raw file contains 32bit quantities
        num keys
        num links
        num keys*key->offset1
        num_keys*offset1->offset2
        num_links*links
        """
    global last
    t0=time.time()
    title2key,key2title=parse_page(page_sql)
    print 'Found %d pages' % len(title2key)
    print time.time()-t0
    print 'Pickling'
    out=open(outname,'wb')
    cPickle.dump(key2title,out,2)
    out.close()
    print time.time()-t0
    print 'Pickled'
    D=defaultdict(list)
    def actionD(pg_from,pg_to):
        D[pg_from].append(pg_to)
    L=array('i')
    Akeys=array('i')
    Aoffsets=array('i')
    Alinks=array('i')
    last=-1
    def action(pg_from,pg_to):
        global last
        if pg_from!=last:
            Akeys.append(pg_from)
            Aoffsets.append(len(Alinks))
            last=pg_from
        Alinks.append(pg_to)
    parse_links(pagelinks_sql,title2key,action)
    print time.time()-t0
    print 'Saving'
    out2=open(outname2,'wb')
    #cPickle.dump(D,out2,2)
    L.append(len(Aoffsets))
    L.append(len(Alinks))
    L.tofile(out2)
    Akeys.tofile(out2)
    Aoffsets.tofile(out2)
    Alinks.tofile(out2)
    out2.close()
    print time.time()-t0

def load_pages():
    """Loads the key->title pickled file"""
    t0=time.time()
    out=open(outname,'rb')
    key2title=cPickle.load(out)
    out.close()
    print 'Loaded key->title in ',time.time()-t0
    return key2title

def load_links():
    """Loads the arrays in the .raw file produced by extract_links
    The .raw file contains 32bit quantities
        num keys
        num links
        num keys*key->offset1
        num_keys*offset1->offset2
        num_links*links
        """
    t0=time.time()
    print 'Loading database file (may take a few seconds)'
    L=array('i')
    Akeys=array('i')
    Aoffsets=array('i')
    Alinks=array('i')
    out2=open(outname2,'rb')
    L.fromfile(out2,2)
    Akeys.fromfile(out2,L[0])
    Aoffsets.fromfile(out2,L[0])
    Alinks.fromfile(out2,L[1])
    out2.close()
    print  'Loaded link database in ',time.time()-t0
    return Akeys,Aoffsets,Alinks

def toplinks(database,top=100):
    """Returns a generator for page ids with more than N outward links"""
    Akeys,Aoffsets,Alinks=database
    for i,start in enumerate(Aoffsets[:-1]):
        x=Aoffsets[i+1]-start
        if x>=top:
            yield Akeys[i],x
    
def makekey2id(database):
    """Returns a generator for page ids with more than N outward links"""
    Akeys,Aoffsets,Alinks=database
    D={}
    for i,key in enumerate(Akeys):
        D[key]=i
    return D
            
# We only convert the file the first time
if not os.path.exists(outname2):
    print "Converting database file, estimated time 10 minutes per gigabyte of compressed database file"
    extract_links() 
database=load_links()
Akeys,Aoffsets,Alinks=database

key2id=makekey2id(database)

key2title=load_pages()
for key,x in toplinks(database,2000):
    try:
        print key,key2title[key],x
    except KeyError:
        pass
#fd.close()

def viewlinks(key):
    i=key2id[key]
    for off in xrange(Aoffsets[i],Aoffsets[i+1]):
        print key2title[Alinks[off]]

def links(key):
    """Return geenrator for all links from this page"""
    try:
        i=key2id[key]
        for off in xrange(Aoffsets[i],Aoffsets[i+1]):
            yield Alinks[off]
    except KeyError,IndexError:
        pass
            
from heapq import *
from collections import deque
import sys
sys.setrecursionlimit(10**5)
def shortest_path(src_key):
    # Breadth first search to find shortest path
    print 'Find largest link'
    cost=array('H')
    prev=array('i')
    M=max(Akeys)
    print 'Prepare array of distances'
    cost.extend([0]*(M+1))
    prev.extend([0]*(M+1))
    print 'Searching'
    H=deque()
    H.append(src_key)
    while len(H):
        key=H.popleft()
        d=cost[key]+1
        for k in links(key):
            try:
                if cost[k]: continue
                cost[k]=d
                prev[k]=key
                H.append(k)
            except IndexError:
                pass
    cost[src_key]=0
    prev[src_key]=0
    return cost,prev

cost,prev=shortest_path(17275600)

def print_path(key):
    while key:
        print key2title[key]
        key=prev[key]
        
print_path(559997)
    
