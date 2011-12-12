""" 
   This module contains two class : MySQLExporter and MySQLImporter.
   These objects are used to insert and extract information to and from a MySQL database
"""

import _mysql
import _mysql_exceptions

"""
   This is the exporter. It inserts data in the MySQL database.
"""

class MySQLExporter(object) :

    def __init__(self, host=None, user=None, passwd=None, db=None, short=True, separator='|', filters=[]) :
        """
           This constructor can initialize the connection to the database.
           if no paramaters are passsed, no connection will be initialized.
           The "short" parameter is used to determine if the insert query should include
           an update statement for duplicate keys. True = no, False = yes. 
           The seperator parameter is used to join lists.
           ex :
           If the item is the following : {a: [42, 1337]}, the final value inserted in the database
           using the default separator will be the following : 42|1337.
           The filters are a list of column names that are used for checking if the row already exists.
           If no filters are set, the exporter will always INSERT.
        """
        self.filters = filters
        self.separator = separator
        self.con = None
        self.isShort = short
        if host is not None and user is not None and passwd is not None and db is not None :
            self.con = _mysql.connect(host=host, user=user, passwd=passwd, db=db)
        self.table_name = None

    def set_filters(self, filters) :
        self.filters = list(filters)

    def get_connection(self) :
        if self.con is None :
            raise AttributeError('Connection has not been initialized')
        return self.con
        
    def set_connection(self, con) :
        """
           This is a simple method to use a connection already established. 
        """
        if type(con).__name__ == 'connection' :
            self.con = con
        else :
            raise TypeError('The connection must be a _mysql connection object') 


    def set_table(self, table) :
        if type(table).__name__ != 'str' :
            raise TypeError('Table name must be a string')
        self.table_name = table


    def build_where_condition(self, item) :
        """
           This method creates the WHERE part of a query.
           The shape of such a part is based on the keys and values
           of the item paramater :
           WHERE key1=value1 AND key2=value2 AND ...
        """
        if len(self.filters) > 0 :
            l = len(self.filters)
            condition = " WHERE "
            for i in range(0, l) :
                if self.filters[i] in item :
                    condition += self.filters[i] + '="' + unicode(item[self.filters[i]]).encode('utf-8') + '"'
                    if i < l - 1 :
                        condition += ' AND '
            return condition
        else :
            return ""


    def create_insert_query(self, item) :
        """
           This method creates an insert query from the item parameter.
           It uses the keys and values of the item :
           INSERT INTO [table_name] (key1, key2, ...) VALUES(value1, value2, ...)
        """
        self.query = 'INSERT INTO ' + self.table_name + ' ('
        self.query += ', '.join(item.keys()) + ') VALUES('
        l = len(item)
        i = 0
        for index in item :
            if hasattr(item[index], '__iter__') :
                self.query += '\"' + self.separator.join(item[index]).encode("utf-8") + '\"'
            else :
                self.query += '\"' + str(item[index]).encode("utf-8") + '\"'
            if i < l - 1 :
                self.query += ', '
            i += 1
        self.query += ')'
        """
           If the short attribute is set to False,
           this is where the ON DUPLICATE KEY UPDATE statement is added.
        """
        if not self.isShort :
            self.query += ' ON DUPLICATE KEY UPDATE '
            i = 0
            for index in item :
                self.query += '`' + index + '`'
                if hasattr(item[index], '__iter__') :
                    self.query += '\"' + self.separator.join(item[index]).encode("utf-8") + '\"'
                else :
                    self.query += '\"' + str(item[index]).encode("utf-8") + '\"'
                if i < l - 1 :
                    self.query += ', '
                i += 1
            

    def create_update_query(self, item) :
        """
           This method creates an UPDATE query from the item parameter :
           UPDATE [table] SET `key1`="value1", `key2`="value2" ...
        """
        l = len(item)
        i = 0
        self.query = 'UPDATE ' + self.table_name + ' SET '
        for index in item :
            self.query += '`' + index + '`'
            if hasattr(item[index], '__iter__') :
                self.query += '=\"' + self.separator.join(item[index]).encode("utf-8") + '\"'
            else :
                self.query += '=\"' + str(item[index]).encode("utf-8") + '\"'
            if i < l - 1 :
                self.query += ', '
            i += 1
        self.query += self.build_where_condition(item)

    def check_for_update(self, item, rows) :
        """
           This method will check from a previous SELECT query
           if there is anything to change. It simply compares
           what has been returned with the item in paramater.
           Returns True if there are changes, False otherwise.
        """
        row = rows.fetch_row(how=1)[0]
        for index in row :
            if index in item :
                if hasattr(item[index], '__iter__') and unicode(self.separator.join(item[index])).encode("utf-8") != row[index]:
                    return True
                elif str(item[index]).encode("utf-8") != row[index]:
                    return True
        return False

    def export_item(self, item):
        """
           This method is where all the above methods are called.
           If the row already exists and there are any filters, the check_for_update method will be called
           to see if there are any changes. Otherwise a INSERT query is created.
           The final query is then executed.
        """
        if self.con is None :
            raise AttributeError('Connection has not been initialized')
        if self.table_name is None :
            raise AttributeError('Table has not been set')
        if type(item).__name__ != 'dict' :
            raise TypeError('Item must be a dictionary')
        if len(item) == 0 :
            pass
        self.con.query('SELECT * FROM ' + self.table_name + ' ' + self.build_where_condition(item))
        rows = self.con.store_result()
        if rows.num_rows() != 0 :
            if self.check_for_update(item, rows) :
                self.create_update_query(item)
                self.con.query(self.query)
        else :
            self.create_insert_query(item)
            self.con.query(self.query)
            

"""
   This is the importer. This class extracts information from the database.
"""

class           MySQLImporter(object) :
    
    def         __init__(self, host=None, user=None, passwd=None, db=None) :
        """
           This constructor can initialize the connection to the database
           if the proper arguments are transfered to it.
        """
        self.con = None
        if host is not None and user is not None and passwd is not None and db is not None :
            self.con = _mysql.connect(host = host, user = user, passwd = passwd, db = db)
        self.table = None


    def         get_connection(self) :
        if self.con is None :
            raise AttributeError('Connection has not been initialized')
        return self.con


    def         set_connection(self, con) :
        """
           This method sets the used connection with an _mysql connection
           that has already been established
        """
        if type(con).__name__ != 'connection' :
            raise TypeError('The connection must be a _mysql connection object')
        self.con = con


    def         set_table(self, table) :
        if type(table).__name__ != 'str' :
            raise TypeError('Table name must be a string')
        self.table = table


    def         _build_where_condition(self, conditions) :
        """
           This method creates the WHERE part of the query from
           the conditions (which are stored in a dictionary).
           it looks like this :
           WHERE key1=value1 AND key2=value2 AND ...
           If there are no conditions, this method will return an empty string
        """
        if len(conditions) > 0 :
            where_condition = " WHERE "
            l = len(conditions.keys())
            i = 0
            for key in conditions :
                where_condition += key + "=\"" + unicode(conditions[key]).encode("utf-8") + '\"'
                if i < l - 1 :
                    where_condition += " AND "
                i += 1
            return where_condition
        else :
            return ""

    def         get_fields(self, columns = ["*"], conditions = {}) :
        """
           This methods performs a SELECT query which will extract the desired information
           from the database. It returns a list of dictionaries, each dictionary being a row.
        """
        result_list = []
        if self.con is None :
            raise AttributeError('Connection has not been initialized')
        if self.table is None :
            raise AttributeError('Table has not been set')
        if len(columns) == 0 :
            columns = ["*"]
        self.con.query("SELECT " + ', '.join(columns) + " FROM " + self.table + self._build_where_condition(conditions))
        result = self.con.store_result()
        return list(result.fetch_row(maxrows=0, how=1))
