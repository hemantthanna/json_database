package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)


const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	} 

	Driver struct {
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log Logger
	}
)

type Options struct{
	Logger

}



/// initialize stuff and create database
func New(dir string, options *Options)(*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}

	if options != nil{
		opts = *options
	}

	if opts.Logger == nil{
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir: dir,
		mutexes: make(map[string]*sync.Mutex),
		log: opts.Logger,


	}

	/// check if this dir derectory exist or not
	if _, err := os.Stat(dir); err == nil{
		opts.Logger.Debug("using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("creating the database at '%s' ...\n", dir)
	/// gettomg excess permission
	return &driver, os.MkdirAll(dir, 0755)
}



/// write entries (Struct method)
func (d *Driver) Write(collection , resource string, v interface{}) error{
	if collection == ""{
		return fmt.Errorf("Missing collection - no place to save record!")
	}
	if resource == ""{
		return fmt.Errorf("missing resource - unable to save record (no name)! ")
	}

	mutex := d.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()


	dir := filepath.Join(d.dir, collection)
	finalPath := filepath.Join(dir, resource+".json")
	temporaryPath := finalPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil{
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))
	if err:= ioutil.WriteFile(temporaryPath, b, 0644); err != nil{
		return err
	}

	return os.Rename(temporaryPath, finalPath)

}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == ""{
		return fmt.Errorf("Missing collection - unable to read!")

	}
	if resource  ==  ""{
		return fmt.Errorf("missing resource - unable to read record (no name)!")


	}

	record := filepath.Join(d.dir, collection, resource)
	
	if _, err := stat(record); err != nil{
		return err
	}
	 b,err  := ioutil.ReadFile(record + ".json")
	 if err != nil {
		return err
	 }

	 return json.Unmarshal(b, &v)

}

func (d *Driver) ReadAll (collection string)([]string, error){

	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to read!")
		
	}

	/// create a absolute file path by combining d.dir (project path) and collection name
	/// eg. C:user/rog/go_database and /users
	dir := filepath.Join(d.dir, collection)
	
	if _, err := stat(dir); err != nil{
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)
	var records []string

	for _,file := range files{
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil

}

func (d *Driver) Delete(collection, resource string) error{

	path := filepath.Join(collection, resource)
	mutex := d.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err!= nil:
		return fmt.Errorf("unable to find file or directory named %v \n", path)
	case fi.Mode().IsDir():
	  	return os.RemoveAll(dir) 
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) GetOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]
	if !ok{
		m= &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

// check for a .json file
func stat(path string) (fi os.FileInfo, err error){
	if fi, err = os.Stat(path); os.IsNotExist(err){
		fi, err = os.Stat(path + ".json")
	}
	return
}

type Address struct{
	City string
	State string
	Country string
	Pincode json.Number
}


type User struct {
	Name string
	Age  json.Number
	Contact string
	Company string
	Address Address
}

func main() {

	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		println("error: ",err)
	}



	/// dummy data TODO: implement a post api which takes user details as input.
	/// generate a query system.
	employees := []User{
		{"John", "23", "6464646464", "golang dev community", Address{"banglore", "karnataka", "india", "111111"}},
		{"ray", "78", "6464646464", "golang dev community", Address{"san francisco", "karnataka", "india", "456224"}},
		{"mingle", "16", "6464646464", "Google", Address{"banglore", "karnataka", "india", "456224"}},
		{"Paul", "23", "6464646464", "Facebook", Address{"banglore", "karnataka", "USA", "456224"}},
		{"Pomodo", "23", "6464646464", "Remote-Teams", Address{"banglore", "karnataka", "india", "456224"}},
		{"Tanjiro", "88", "6464646464", "Dominate", Address{"banglore", "Mumbai", "india", "333333"}},

	}

	/// inputting the values into database
	/// writing in folder [users] and file with name value.Name
	for _, value := range employees{
		db.Write("users", value.Name, User{
			Name: value.Name,
			Age: value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}


	/// reading all records in users folder.
	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("error: ", err)
	}

	fmt.Println(records)

	/// unmarshaling json to user object slice
	allUsers := []User{}

	for _, f := range records{
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil{

			fmt.Println("error: ", err)
		}

		allUsers = append(allUsers, employeeFound)

	}
	fmt.Println(allUsers)



	// if err := db.Delete("users", "John"); err != nil{
	// 	fmt.Println("error:", err)
	// }
	
	// if err := db.Delete("users", ""); err != nil{
	// 	fmt.Println("error:", err)
	// }


}