from multiprocessing import shared_memory
from myThread import NewThread
import threading
import os.path
import time

class DataBase():
    # shared memory indexes
    # (__, __) -> [0] permission index, [1] lock inde, 
    PERM, LOCKED = (0, 1)

    READ, WRITE = (n for n in range(0, 4, 2)) 
    
    def __init__(self, db_path="self.txt", changes_path="db_changes.txt"):
        self.db_path = db_path
        self.changes_path = changes_path

        self.lock = shared_memory.SharedMemory(name="lock_read_write", create=True, size=4)
        
        # when using the lock, each thread/process add one to the buffer and saves the number it was on. when the variable returns to be the number it was it the threads turn
        # when leaving each process decrease one of the lock_buffer
        self.lock_buffer = self.lock.buf
        self.lock_buffer[0] = 0 # lock for reading from the database
        self.lock_buffer[1] = 0 # lock for reading from the database
        self.lock_buffer[2] = 0 # lock for writing to the database
        self.lock_buffer[3] = 0 # lock for writing to the database
        
        if not os.path.exists(self.db_path):
            with open(self.db_path, 'w'):
                pass
        if not os.path.exists(self.changes_path):
            with open (self.changes_path, 'w'):
                pass
    
    def wait_until(self, queue_num, prop):
        """Function waits until its property queue equal to the asker num

        Args:
            queue_num (int): client queue num
            prop (int): the index to the property wanted

        Returns:
            bool: True if queue num is the client's one, False otherwise
        """
        must_end = time.time() + 1 # timeout for the request
        while time.time() < must_end:
            if queue_num == self.lock_buffer[prop + DataBase.PERM]:
                return True
            time.sleep(0.05)
        return False

    def __lock_prop(self, prop):
        """locking property and give permission.

        Args:
            prop (int): the property index in shared memory
        """
        self.__get_permission(prop) # if not locked and append one to the property
        
        self.lock_buffer[prop + DataBase.LOCKED] += 1
        
        # self.lock_buffer[prop] += 1
        queue_num = self.lock_buffer[prop]
        wait_thread = threading.Thread(target=self.wait_until, args=(queue_num, prop))
        wait_thread.start()
        succeed = wait_thread.join()
        while not succeed:
            wait_thread = threading.Thread(target=self.wait_until, args=(queue_num, prop))
            wait_thread.start()
            succeed = wait_thread.join()


    def __get_permission(self, prop):
        """Gives premition if property isn't locked it adds one user to the property.

        Args:
            prop (int): the index of the property

        Returns:
            bool: if permition have been granted
        """
        self.lock_buffer[prop] += 1
        queue_num = self.lock_buffer[prop]

        while self.lock_buffer[prop + DataBase.LOCKED]:
            wait_thread = NewThread(target=self.wait_until, args=())
            wait_thread = threading.Thread(target=self.wait_until, args=(queue_num, prop))
            wait_thread.start()
            succeed = wait_thread.join()
        
        
        return True


    def __release(self, prop):
        self.lock_buffer[prop + DataBase.PERM] -= 1
        self.lock_buffer[prop + DataBase.LOCKED] = 0
    
            
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
    
    def __union(self):
        """Merges the changes into the database
        """
        db = DataBase.read2dict(self.db_path)
        self.__lock_prop(DataBase.WRITE) # lock for writing until finished saving everything
            
        changes = DataBase.read2dict(self.changes_path)
        
        updated = {**db, **changes}
        for key, value in dict(updated).items():
            if value == 'None':
                del updated[key]
        self.__lock_prop(DataBase.READ)
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
            time.sleep(0.02)
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

    # def update(self, key, new_value=None):
    #     """Update the changes file by the key and value received, if value is None, key deleted
    #     Args:
    #         key (str): key
    #         value (str): value
    #     """
    #     self.__lock_prop(DataBase.WRITE)
    #     self.__lock_prop(DataBase.READ)
    #     try:
    #         succeed = False
    #         with open(self.db_path, 'r+') as db:

    #             data = db.readlines()
    #             for line in range(len(data)):
    #                 if data[line].startswith(key):
    #                     if new_value != None:
    #                         data[line] = data[line][:len(key)+1] + new_value + "\n"
    #                     else:
    #                         del data[line]
    #                     succeed = True
    #                     break
            
    #         with open(self.db_path, 'w') as f:
    #             f.writelines(data)
            
    #         return succeed
    #     finally:
    #         self.__release(DataBase.WRITE)
    #         self.__release(DataBase.READ)

    # def replace(self, key, new_value=None):
    #     succeed = False
    #     with open(self.db_path, 'r+') as db:

    #         data = db.readlines()
    #         for line in range(len(data)):
    #             if data[line].startswith(key):
    #                 if new_value != None:
    #                     data[line] = data[line][:len(key)+1] + new_value + "\n"
    #                 else:
    #                     del data[line]
    #                 succeed = True
    #                 break
        
    #     with open(self.db_path, 'w') as f:
    #         f.writelines(data)
        
    #     return succeed
    
            
    def append(self, key, value):
        self.__lock_prop(DataBase.READ)
        self.__lock_prop(DataBase.WRITE)
        with open(self.db_path, 'a') as f:
            f.write(key + ':' + value + '\n')
        self.__release(DataBase.READ)
        self.__release(DataBase.WRITE)
            
            
if __name__ == "__main__":
    db = DataBase()
    db.append('a', 'X')
    db.append('b', 'Y')
    db.replace('a', 'Z')
    print(db.read('a'))
    print(db.read('b'))
    