"""
Testing the dynamodb module
"""

import dynamodb

def main():
    region="eu-west-1"
    con = dynamodb.connect_dynamodb(region)

    table = dynamodb.get_table(con, "testingc")
    print table 

    the_item = dynamodb.get_item(table, 1)
    print the_item

    item_attrs = {}
    item_attrs['length'] = 177 
    item_attrs['name'] = "kerst" 
    item_attrs['lastname'] = "man" 
    item_attrs['city'] = "zuidpool" 
    result = dynamodb.set_item(table, 5, item_attrs)

    print result

if __name__ == "__main__":
    main()
