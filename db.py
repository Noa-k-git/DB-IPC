class DataBase():
    file_path = "database.txt"

    @staticmethod
    def read(key):
        with open(DataBase.file_path, 'r') as db:
            while True:
                line = db.readline()
                if not line:
                    break

                if line.startswith(key):
                    return(line[len(key)+1:-1])
        return None

    @staticmethod
    def write(key, value):
        with open(DataBase.file_path, 'r') as db:
            data = db.readlines()
        for line in range(len(data)):
            if data[line].startswith(key):
                    data[line] = line[:len(key)+1] + value + "\n"
                    break
        
        with open(DataBase.file_path, 'w') as f:
            f.writelines(data)
        
        return
    
    @staticmethod
    def insert(key, value):
        with open(DataBase.file_path, 'a') as f:
            f.write(key + ':' + value + '\n')