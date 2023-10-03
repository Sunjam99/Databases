import sqlite3
databaseName = "shipment_database.db"

con = sqlite3.connect(databaseName)
cur = con.cursor()

def getId(table):
    res = cur.execute(f"SELECT MAX(id) FROM {table}")
    max_id = res.fetchone()[0]
    if (max_id is None):
        return 0
    else:
        return max_id+1

def newProduct(name):
    res = cur.execute(f"SELECT * FROM product WHERE name = '{name}'")
    return res.fetchone() is None

def getProdId(name):
    res = cur.execute(f"SELECT id FROM product WHERE name = '{name}'")
    return res.fetchone()[0]

def insert_shipments(filepath):
    ship_id = getId("shipment")
    prod_id = getId("product")
    
    with open(filepath) as f:
        lines = f.readlines()
        for line in lines[1:]:
            values = line.split(',')
            origin = values[0]
            destination = values[1]
            product = values[2]
            
            quantity = values[4]
            
            if newProduct(product):
                cur.execute("INSERT INTO product VALUES (?,?)", (prod_id,product))
                prod_id += 1            
            cur.execute(f"INSERT INTO shipment VALUES (?,?,?,?,?)", (ship_id,getProdId(product),quantity,origin,destination))
            ship_id += 1
    con.commit()

def insert_shipments2(filepath1,filepath2):
    ship_id = getId("shipment")
    prod_id = getId("product")
    
    items = dict()
    quantitys = dict()
    
    with open(filepath1) as f:
        lines = f.readlines()
        for line in lines[1:]:
            values = line.split(',')
            shipment = values[0]
            product = values[1]
            if newProduct(product):
                cur.execute("INSERT INTO product VALUES (?,?)", (prod_id,product))
                prod_id += 1  
            if shipment in quantitys:
                quantitys[shipment] += 1
            else:
                items[shipment] = product
                quantitys[shipment] = 1
    
    with open(filepath2) as f:
        lines = f.readlines()
        for line in lines[1:]:
            values = line.split(',')
            shipment = values[0]
            origin = values[1]
            destination = values[2]
            
            cur.execute(f"INSERT INTO shipment VALUES (?,?,?,?,?)", (ship_id,getProdId(items[shipment]),quantitys[shipment],origin,destination))
            ship_id += 1
    con.commit()

insert_shipments("data/shipping_data_0.csv")
insert_shipments2("data/shipping_data_1.csv","data/shipping_data_2.csv")