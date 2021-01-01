import json
import os
import sys
import time
import threading
class datastore:

    def __init__(self, file_path=os.getcwd()):

        self.filepath = file_path + '/datastore.json'
        self.filelock = threading.Lock()
        self.datalock = threading.Lock()

        try:
            file = open(self.filepath, 'r')
            filedata = json.load(file)
            self.data = filedata
            file.close()

            if not self.checkfilesize():
                raise Exception('Size of the data store exceeds 1 GB. Unable to add more data.')

            print('Database opened in directory, ' + self.filepath)
        except:

            file = open(self.filepath, 'w')
            self.data = {}
            self.ttldict = {}
            file.close()
            print('Database created in directory, ' + self.filepath)

                                                                               # checks if the database exists in the directory. 
                                                                               #If it does, the database is opened and built, Otherwise, it just creates a new database.

    def checkfilesize(self):
                                                                                 # Checks if database size exceeds 1 GB size.
        self.filelock.acquire()

        if os.path.getsize(self.filepath) <= 1e+9:
            self.filelock.release()
            return True
        else:
            self.filelock.release()
            return False

        
    def verifykey(self, key):                                                    
           
        if type(key) == type(""):                                                  # Checks if key meets required conditions of being a string and capped at 32 chars.
            if len(key) > 32:
                raise Exception('Key size is capped at 32. Given key length is ' + str(len(key)))
            else:
                return True
        else:
            raise Exception('Key value is not a string. Inputed key is of type: ' + str(type(key)))
            return False



    def Create(self, key='', value='', ttl=None):

        self.verifykey(key)

        if key == '':
            raise Exception('No key was provided.')

        if value == '':
            value = None

       
        if sys.getsizeof(value) > 15000:                                           # Checks if the size of the value exceeds 15 KB.
            raise Exception("Size of the value exceeds 15KB size limit.")

        if not self.checkfilesize():
            raise Exception('Size of the data store exceeds 1 GB. Unable to add more data.')

        self.datalock.acquire()
        if key in self.data.keys():
            self.datalock.release()
            raise Exception('Key is already present.')

                                                                                    
        if ttl is not None:                                                         # ttl = Time-To-Live. parameter ttl stores the time till which the data is allowed to read and delete.

        ttl = int(time.time()) + abs(int(ttl))

        tempdict = {'value': value, 'ttl': ttl}
        self.data[key] = tempdict

        self.filelock.acquire()
        json.dump(self.data, fp=open(self.filepath, 'w'), indent=2)

        self.filelock.release()
        self.datalock.release()

        print('Value added')



    def Read(self, key=''):                                                           # Finds the key in the data store and displays the value, if the ttl has not crossed.

        self.verifykey(key)

        if key == '':
            raise Exception('Expecting a key to be read.')

        self.datalock.acquire()

        if key in self.data.keys():
            pass
        else:
            self.datalock.release()
            raise Exception('Key not found in database')

        ttl = self.data[key]['ttl']

        # Checks if ttl was provided.
        if not ttl:
            ttl = 0

        # if the ttl was crossed, an error is raised

        if (time.time() < ttl) or (ttl == 0):
            self.datalock.release()
            return json.dumps(self.data[key]['value'])


        else:
            self.datalock.release()
            raise Exception("Key's Time-To-Live has expired. Unable to read.")



    def Delete(self, key=''):                                                          # Finds whether key is present and if,then deletes it from the data store.


        self.verifykey(key)

        if key == '':
            raise Exception('Expecting a key to be read.')

        self.datalock.acquire()

        if key in self.data.keys():
            pass
        else:
            self.datalock.release()
            raise Exception('Key not found in database')

        ttl = self.data[key]['ttl']
        if not ttl:                                                                     # Checks if ttl was given
            ttl = 0

        if time.time() < ttl or (ttl is 0):

            self.data.pop(key)

            self.filelock.acquire()
            file = open(self.filepath, 'w')
            json.dump(self.data, file)

            self.filelock.release()
            self.datalock.release()

            print("Key-value pair deleted")
            return
        else:
            self.datalock.release()
            raise Exception("Key's Time-To-Live has expired. Unable to delete!!")

  
            
 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
