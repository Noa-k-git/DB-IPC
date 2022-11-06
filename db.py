from multiprocessing import shared_memory
import threading
import os.path
import time

class DataBase():
    READ = 0
    READ_LOCKED = 1
    WRITE = 2
    
    def __init__(self, db_path="self.txt", changes_path="db_changes.txt"):
        self.db_path = db_path
        self.changes_path = changes_path

        self.lock = shared_memory.SharedMemory(name="lock_read_write", create=True, size=2)
        
        # when using the lock, each thread/process add one to the buffer and saves the number it was on. when the variable returns to be the number it was it the threads turn
        # when leaving each process decrease one of the lock_buffer
        self.lock_buffer = self.lock.buf
        self.lock_buffer[0] = 0 # lock for reading from the database
        self.lock_buffer[1] = 0 # lock for reading from the database
        self.lock_buffer[2] = 0 # lock for writing to the database
        
        if not os.path.exists(self.db_path):
            with open(self.db_path, 'w'):
                pass
        if not os.path.exists(self.changes_path):
            with open (self.changes_path, 'w'):
                pass
    
    def wait_until(self, queue_num, prop):
        must_end = time.time() + 10 # timeout for the request
        while time.time() < must_end:
            if queue_num == self.lock_buffer[prop]:
                return True
            time.sleep(0.1)
        return False
    

    def lock_prop(self, prop):
        queue_num = self.lock_buffer[prop]
        self.lock_buffer[prop] += 1
        
        wait_thread = threading.Thread(target=self.wait_until, args=(self, queue_num, prop))
        wait_thread.start()
        wait_thread.join()
    
    def get_permission(self, prop):
        if prop == DataBase.WRITE:
            self.lock_prop(prop)
        else:
            
            self.lock_buffer[prop] += 1
    
    def release(self, prop):
        self.lock_buffer[prop] -= 1
    
    
            
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
                line = db.readline()
                if not line:
                    break
                
                key, value = line.split(":", 2)
                db[key] = line[value]
                
        return new_dict
    
    def dict2file(self, new_dict):
        """Updates the database from the dict received

        Args:
            new_dict (dict): dict with updated values
        """
        with open(self.db_path, 'w') as db:
            for key, value in new_dict.items(): 
                db.write('%s:%s\n' % (key, value))
    
    def _union(self):
        """Merges the changes into the database
        """
        db = DataBase.read2dict(self.db_path)
        succeed = self.lock_prop(DataBase.WRITE) # lock for writing until finished saving everything
        while not succeed:
            self.lock_prop()
            
        changes = DataBase.read2dict(self.changes_path)
        
        updated = {**db, **changes}
        succeed = self.lock_prop(DataBase.READ)
        self.dict2file(updated)
        with open(self.changes_path, 'w'):
            pass
        self.release(DataBase.READ)
        self.release(DataBase.WRITE)
        
    def read(self, key):
        """Reads the content of the database and return content of the wanted value

        Args:
            key (str): the key of the value

        Returns:
            str: the wanted value
        """
        succeed = self.get_permission(DataBase.READ)
        while not succeed:
            self.get_permission(DataBase.READ)
        try:
            with open(self.changes_path, 'r') as db:
                lines = db.readlines()
                for line in reversed(lines):
                    curr_key, curr_value = line.split(":", 2)
                    if curr_key == key:
                        return curr_value
                    
            with open(self.db_path, 'r') as db:
                while True:
                    line = db.readline()
                    print(db.tell())
                    if not line:
                        break
                    
                    curr_key, curr_value = line.split(":", 2)
                    if curr_key == key:
                        return curr_value
            return None
        finally:
            self.release(DataBase.READ)

    def change(self, key, value):
        """Update the changes file by the key and value received

        Args:
            key (str): key
            value (str): value
        """
        self.get_permission(DataBase.WRITE)
        
    
    def replace(self, key, new_value=None):
        succeed = False
        with open(self.db_path, 'r+') as db:

            data = db.readlines()
            for line in range(len(data)):
                if data[line].startswith(key):
                    if new_value != None:
                        data[line] = data[line][:len(key)+1] + new_value + "\n"
                    else:
                        del data[line]
                    succeed = True
                    break
        
        with open(self.db_path, 'w') as f:
            f.writelines(data)
        
        return succeed
        
        with open(self.db_path, 'w') as f:
            f.writelines(data)
        
        return succeed
            
    def append(self, key, value):
        with open(self.db_path, 'a') as f:
            f.write(key + ':' + value + '\n')
            
            
if __name__ == "__main__":
    # self.append('a', 'X')
    # self.append('b', 'Y')
    # self.replace('a', 'Z')
    # print(self.read('a'))
    # print(self.read('b'))
    