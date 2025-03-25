
db = db.getSiblingDB('testdb');
db.users.insertMany([
    { name: "Alice", age: 30 },
    { name: "Bob", age: 25 },
    { name: "Charlie", age: 35 }
]);
