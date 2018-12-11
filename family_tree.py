from person import person

import sqlite3

conn = sqlite3.connect('family_tree.db')


def create_table_person():
    with conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE person (
                    first_name text,
                    last_name text,
                    id integer PRIMARY KEY
                    )""")
    

def create_table_family_connection():
    with conn:
        c = conn.cursor()    
        c.execute("""CREATE TABLE family_connection (
                    id_first integer,
                    id_second integer,
                    connection integer,
                    FOREIGN KEY(id_first) REFERENCES person(id),
                    FOREIGN KEY(id_second) REFERENCES person(id),
                    PRIMARY KEY(id_first, id_second, connection)
                    )""")    

def delete_table(table):
    with conn:
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS %s" % table)
        
def insert_into_person(per):
    try:
        with conn:
            c = conn.cursor()    
            c.execute("""INSERT INTO person(first_name, last_name, id)
                      VALUES(?, ?, ?)""" , (per.first, per.last, per.person_id))
    except Exception as e:
        print "Can't insert"
        print e

def insert_mother(per, mother):
    try:
        per.update_mother(mother)
        with conn:
            c = conn.cursor()    
            c.execute("""INSERT INTO family_connection(id_first, id_second, connection)
                      VALUES(?, ?, ?)""" , (per.person_id, mother.person_id, 2)) # 2 is the connection for mother
    except Exception as e:
        print "Can't insert"
        print e     
        
def insert_father(per, father):
    try:
        per.update_father(father)
        with conn:
            c = conn.cursor()    
            c.execute("""INSERT INTO family_connection(id_first, id_second, connection)
                      VALUES(?, ?, ?)""" , (per.person_id, father.person_id, 1)) # 1 is the connection for father
    except Exception as e:
        print "Can't insert"
        print e        

def find_sibilings(per):
    if per.mother is None and per.father is None:
        return []
    if per.father is None:
            return find_sibilings_through_mother(per)
    if per.mother is None:
        return find_sibilings_through_father(per)
    with conn:
        c = conn.cursor()    
        c.execute("""SELECT f.id_first, p.first_name, p.last_name FROM family_connection AS f
                    JOIN person AS p
                    ON p.id = f.id_first
                    WHERE f.id_first != ? AND (f.id_second = ? OR f.id_second = ?)     
                   """, (per.person_id, per.mother.person_id, per.father.person_id))
        return c.fetchall()

def find_sibilings_through_mother(per):
    if per.mother is None:
        return []
    with conn:
        c = conn.cursor()    
        c.execute("""SELECT f.id_first, p.first_name, p.last_name FROM family_connection AS f
                    JOIN person AS p
                    ON p.id = f.id_first
                    WHERE f.id_first != ? AND f.id_second = ?     
                  """, (per.person_id, per.mother.person_id))
        return c.fetchall()

def find_sibilings_through_father(per):
    if per.father is None:
        return []
    with conn:
        c = conn.cursor()    
        c.execute("""SELECT f.id_first, p.first_name, p.last_name FROM family_connection AS f
                    JOIN person AS p
                    ON p.id = f.id_first
                    WHERE f.id_first != ? AND f.id_second = ?     
                  """, (per.person_id, per.father.person_id))
        return c.fetchall()
