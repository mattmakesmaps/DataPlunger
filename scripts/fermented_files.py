__author__ = 'matt'
__date__ = '5/8/14'

import pickle as cPickle, os

if __name__ == '__main__':
    pathway = '/Users/matt/Projects/dataplunger/scripts/temp/pickled.pickle'
    with open(pathway, 'w+b') as p_handle:
        print "dumping"
        v1 = cPickle.dumps('hello')
        v2 = cPickle.dumps(['1',2,'three'])
        v3 = cPickle.dumps({'key':'value'})
        p_handle.writelines([v1,v2,v3])

    with open(pathway, 'r+b') as p_handle2:
        print "loading"
        for i in p_handle2:
            try:
                print cPickle.loads(i)
            except EOFError:
                break
