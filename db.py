import threading
import os.path
import time

class DataBase():    
    def __init__(self, db_path="database.txt", changes_path="db_changes.txt"):
        self.db_path = db_path
        self.changes_path = changes_path
        self.lock = threading.Lock()
        if not os.path.exists(self.db_path):
            with open(self.db_path, 'w'):
                pass
        if not os.path.exists(self.changes_path):
            with open (self.changes_path, 'w'):
                pass
    
            
    @staticmethod
    def read2dict(path):
        """Reads a content from a file specified and converts it to dictionary object

        Args:
            path (str): the path to a database, values are organized key:value

        Returns:
            dict: contains the database
        """
        new_dict = {}
        with open(path, 'r') as db:
            while True:
                line = db.readline().strip()
                if line == '':
                    break
                
                key, value = line.split(":", 2)
                new_dict[key] = value
                
        return new_dict
    
    def dict2file(self, new_dict):
        """Updates the database from the dict received

        Args:
            new_dict (dict): dict with updated values
        """
        with open(self.db_path, 'w') as db:
            for key, value in new_dict.items(): 
                db.write('%s:%s\n' % (key, value))
    
    def merge(self):
        time.sleep(1)
        """Merges the changes into the database
        """
        db = DataBase.read2dict(self.db_path)            
        changes = DataBase.read2dict(self.changes_path)
        
        updated = {**db, **changes}
        for key, value in dict(updated).items():
            if value == 'None':
                del updated[key]
        self.dict2file(updated)
        with open(self.changes_path, 'w'):
            pass

        
    def read(self, key):
        time.sleep(1)
        """Reads the content of the database and return content of the wanted value

        Args:
            key (str): the key of the value

        Returns:
            str: the wanted value
        """

        with open(self.changes_path, 'r') as db:
            lines = db.readlines()
            for line in reversed(lines):
                curr_key, curr_value = line.strip().split(":", 2)
                if curr_key == key:
                    return curr_value
                
        with open(self.db_path, 'r') as db:
            while True:
                line = db.readline()
                if not line:
                    break
                
                curr_key, curr_value = line.split(":", 2)
                if curr_key == key:
                    return curr_value
        return 'None'
            
    def append(self, key, value):
        self.lock.acquire()
        time.sleep(1)
        print(f'active threads: {threading.active_count()}')
        if value == '':
            value = str(None)
        with open(self.changes_path, 'a') as f:
            f.write(key + ':' + value + '\n')
        self.lock.release()
            
if __name__ == "__main__":
    pass