__author__ = 'matt'
__date__ = '3/28/14'

if __name__ == '__main__':
    things = [
        {'name':'Matt','age':27,'pet':'Dooder'},
        {'name':'Bob','age':30,'pet':'Speckles'},
        {'name':'Frank','age':30,'pet':'Norma'},
    ]

    fin_sort = sorted(things, key=lambda thing: thing['name'])
    print fin_sort
