    # Zach Bakke
    # CS-340

from pymongo import MongoClient, errors
from bson.objectid import ObjectId 

class AnimalShelter(object): 
    """ CRUD operations for Animal collection in MongoDB """ 

    def __init__(self): 
        # Initializing the MongoClient. This helps to access the MongoDB 
        # databases and collections. This is hard-wired to use the aac 
        # database, the animals collection, and the aac user. 
        # 
        # You must edit the password below for your environment. 
        # 
        # Connection Variables 
        # 
        USER = 'aacuser' 
        PASS = 'CS340zb!' 
        HOST = 'localhost' 
        PORT = 27017 
        DB = 'aac' 
        COL = 'animals' 
        # 
        # Initialize Connection 
        # 
        self.client = MongoClient(f'mongodb://{USER}:{PASS}@{HOST}:{PORT}') 
        self.database = self.client['%s' % (DB)] 
        self.collection = self.database['%s' % (COL)] 
            
    # Complete this create method to implement the C in CRUD. 
    def create(self, data):
        """
        Insert a document into the MongoDB collection.

        Args:
            data (dict): Key/value pairs acceptable to insert_one().

        Returns:
            bool: True if successful insert, else False.
        """
        if not isinstance(data, dict) or not data:
            # Invalid payload -> fail fast with False per assignment contract
            return False

        try:
            result = self.collection.insert_one(data)
            # Return True if the operation was acknowledged and an _id was generated
            return bool(result.acknowledged and result.inserted_id is not None)
        except errors.PyMongoError:
            # If anything goes wrong with Mongo, return False (no exceptions bubbling up)
            return False


    # Create method to implement the R in CRUD.
    def read(self, query):
        """
        Query documents using find() and return them as a list.

        Args:
            query (dict): Filter dict for find(). Example: {"species": "dog"}

        Returns:
            list: List of documents if successful; empty list on failure.
        """
        try:
            # Ensure query is a dict; default to {} (match-all) if None
            if query is None:
                query = {}
            elif not isinstance(query, dict):
                # Invalid query shape -> return empty list as "failure"
                return []

            cursor = self.collection.find(query)  # MUST use find(), not find_one()
            results = list(cursor)                # realize the cursor
            return results
        except errors.PyMongoError:
            return []

    # Create method to implement the U in CRUD.
    def update(self, query, new_values, many=False, upsert=False):
        """
        Update document(s) that match the query.

        Args:
            query (dict): Filter dict used to select documents to update.
            new_values (dict): Update document acceptable to update_one/update_many.
                               If no operator (e.g., "$set") is provided, this method
                               will wrap it as {"$set": new_values}.
            many (bool): If True, update_many; else update_one. Default False.
            upsert (bool): If True, insert a new doc if none match. Default False.

        Returns:
            int: Number of documents modified in the collection.
        """
        # Validate inputs
        if not isinstance(query, dict) or query is None:
            return 0
        if not isinstance(new_values, dict) or not new_values:
            return 0

        # If caller did not include an update operator, default to $set
        has_operator = any(k.startswith("$") for k in new_values.keys())
        update_doc = new_values if has_operator else {"$set": new_values}

        try:
            if many:
                result = self.collection.update_many(query, update_doc, upsert=upsert)
            else:
                result = self.collection.update_one(query, update_doc, upsert=upsert)

            # Return the number of modified documents (not matched count)
            return int(result.modified_count or 0)
        except errors.PyMongoError:
            return 0

    # Create method to implement the D in CRUD.
    def delete(self, query, many=False):
        """
        Delete document(s) that match the query.

        Args:
            query (dict): Filter dict used to select documents to delete.
            many (bool): If True, delete_many; else delete_one. Default False.

        Returns:
            int: Number of documents removed from the collection.
        """
        if not isinstance(query, dict) or query is None:
            return 0

        try:
            if many:
                result = self.collection.delete_many(query)
            else:
                result = self.collection.delete_one(query)

            # Return number of docs actually deleted
            return int(result.deleted_count or 0)
        except errors.PyMongoError:
            return 0