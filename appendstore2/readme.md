# What is appendstore2?

This library is append-only storage for records of data.
Each record consists of:
- kind : a string identifying the type of data
- meta : optional arbitrary, short string
- timestamp : int64, milliseconds since epoch, either provided by user or set to current time
- data : []byte

Restrictions:
- kind cannot contain spaces and newlines
- meta cannot contain newlines
Those restrictions are consequences of the format of index file.

## Data storage format

Data is stored mostly in 2 files:
- data file : contains data part of chunks
- index file : contains meta information (kind, meta, timestamp) and either a pointer within data file (offset, size) or a size for inline data (i.e. saved in index file)

Optionally, you can save data of record to a separate file.

Index file is human-readable.

Format of index file:

<offset> <size> <timestamp> <kind> <meta>\n
<optional inline data>\n
... repeat

Example:

798 646 1769903131790 user id:1234

Special cases:
- <offset> is '_' : means data is inline, i.e. stored in index file right after this line
- <offset> is 'f' : means data is stored in a separate file, named
  <meta> (meta is mandatory in this case)

Examples:
f 12 1769903131906 attachment doc1.dat
_ 16 1704067200000 log entry1

# But why?

I believe this kind of storage can replace a database for many web applications.

Advantages you should care about:
- faster than a database
- easier to work with than a database

Additional advantages:
- code is 0.1% of size of a database
- robust because simple, append-only format
- easier to work with than a database
- data is in failes, easy to bakck up, easy to move around
- human-readable index file makes inspection easy

Of course replacing PostgreSQL or SQLite with a simple append-only files sounds like a delusion.

If data storage is a Gordian Knot, I'm Alexander.

# How to use appendstore2

If you use a database, you need to design your app and your data in a way that fits relational model.

When using appendstore2, you need to design your app and your data in a way that fits append-only storage.

Here's the main idea:
* all metadata needed for the application to work lives in memory
* large data is read on demand
* on startup you read the whole index and rebuild the state in memory
* during operations you append new records to the store and update state in memory

To make an analogy: in React a rendered html is a state of the data.

With appendstore2 data in memory is a state of the data (which is a sequence of records).

The flexibility and difficulty of using appendstore2 is:
* the format of records is completely up to you
* the way you store data in memory is completely up to you

## Example application

Let's say you build a pastebin-like application. A user can create a paste with some text content, and later retrieve it by id.

So in memory you have:

```go
type User struct {
    ID string `json:"id"`
    Name string `json:"name"`
    CreatedAt time.Time
}

type Paste struct {
    ID string
    UserID string // owner
    Title string
    Type string // e.g. "text", "markdown", "go", "python", etc.
    // we don't store content in memory
    // Content string
    CreatedAt time.Time
}

var (
    users []*User
    pastes []*Paste
    muStore sync.Mutex
    store *appendstore2.Store
)
```

You use a single store for all data.

We have 2 kinds of records: `user` and `paste`.

Here's how we would store them in `appendstore2`:
* User record:
  - kind: "user"
  - meta: "id:<id> name:<name>"
  - timestamp: time of creation i.e. CreatedAt
* Paste record:
  - kind: "paste"
  - meta: "id:<userID-pasteId> title:<title> type:<type>"
  - timestamp: time of creation i.e. CreatedAt
  - data: raw text of the pate

Design decisions.

User data is small, we store it in meta. Another option would be to store is as inline data in e.g. JSON format

Paste content can be large, we store it data file. We store only small metadata in meta field.

Strictly we only need the paste id in memory so we only need to store that in meta field. We also store type for speed and simplicity. That way content is just raw data.

Another option would be to store all non-essential data about paste in data, encoded e.g. as JSON.

Important: we split the data into small "hot" and "cold" data (potentially large). Hot data is stored in memory for fast access. Cold data is stored in appendstore2 for durability.

Hot data allows us to performa common operations without reading from disk:
* we can find a user by id or name (id is stored in unique cookie)
* we can list all users
* we can find all pastes by an user 
* we can find paste by id (e.g. from url)

Even though the way we store pastes is inefficient for finding pastes by user (we have to scan all pastes), it's good enough for millions of pastes. Scanning sequential memory is that fast.

That being said, memory layout is completely up to us. We can speed up lookups by changing memory layout to using maps:
```go
var (
    userByID map[string]*User
    pasteByID map[string]*Paste
    pastesByUserID map[string][]*Paste
)
````
I tend to default to simplicity i.e. arrays.

To store metadata the simplest thing to do is to use JSON. Metadata doesn't allow newlines so you need to use compact JSON encoding.

Personally, for readability I like the "key: value" format so this library includes `KeyValueMarshal` and `KeyValueUnmarshal` for serializing metadata in such format.

```go
const (
    kindUser = "user"
    kindPaste = "paste"	
)
func addUser(name string) (*User, error) {
    muStore.Lock()
    defer muStore.Unlock()

    id := generateID() // implement a function to generate unique IDs
    user := &User{ID: id, Name: name}
    meta, err := json.Marshal(user)
    if err != nil {
        return nil, err
    }
    _, err = store.Append(kindUser, meta, nil)
    if err != nil {
        return nil, err
    }
    // important: we update in memory state in OnRecord()
    return user, nil
}
```

## Updating data

The data on disk is append-only. How do you update a record?

Simple: append latest version.

Let's say you want to change user's name. Append new record with same id but new name.

When rebuilding state in memory, you use the latest record for each id.

## Data evolution

If you ever did an application using a database, you know that data evolution is a pain.

You need to write migration scripts which change the structure of the database and update the data.

With `appendstore2` data evolution is considerably easier.

Adding new metadata fields typically doesn't require any change to data storage.

Let's say you want to add an email field to User.

Your meta filed changes from:
- "id:<id> name:<name>"
- to "id:<id> name:<name> email:<email>"

When rebuilding state in memory, you check if email field is present. If not, you set it to empty string.

Similarly, dropping fields is easy: you just stop using them.

What if you want to radically change the data for user in a way that is hard to fit into existing records?

Create a new kind of record `user-v2` with new format.

When rebuilding state in memory, you check for both `user` and `user-v2` records. If both are present for the same id, you use `user-v2`.

# Scalability

In a database-based application people typically use a single database for all data.

When the data grows very large, they shard the data into multiple databases.

You can shard data with `appendstore2` as well.

For many kinds of applications sharding is easy.

Consider a note taking application like Evernote.

Without sharding, you can store all data for all users in a single store.

With sharding you would split data thusly:
* a main store that contains user records
* data for each user is in a separate store

For simplicity and isolation of user data, you an put store for each user in a separate directory, named based on unique user id.

When a user logs in, you open their store.

To conserve memory you can periodically purge data related to users that are not active e.g. after an hour of inactivity.
