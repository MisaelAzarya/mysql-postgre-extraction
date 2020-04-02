const co = require('co');
const mysql = require('mysql');
const pg = require('pg');
const Q = require("q");

const mysqlConnection = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "supersample"
})

//syntaxnya username:password@server:port/database_name
const pgConString = "postgres://postgres:1234@localhost:5432/supersample"

function mysqlQuery(query) {
  return new Promise((resolve, reject) => {
      setTimeout(() => {
        mysqlConnection.query(query, function(err, rows, fields) {
        if (err) {
            console.error(err);
            return reject(err);
        }
        resolve({ rows, fields });
        })
      }, 5000);
  });
}

function pgQuery(client, query, values) {
  return new Promise((resolve, reject) => {
    client.query(query, values, function(err, results) {
      if (err) {
        console.error(err);
        return reject(err);
      }
      resolve(results)
    })
  })
}

// function while menggunakan promise
function promiseWhile(condition, body) {
    var done = Q.defer();

    function loop() {
        if (!condition()) return done.resolve();
        Q.when(body(), loop, done.reject);
    }
    Q.nextTick(loop);

    // The promise
    return done.promise;
}

var clientpg = new pg.Client(pgConString);
clientpg.connect(function(err){
    if(err) throw err;
    console.log("connect");
    var pgTable = "CREATE TABLE IF NOT EXISTS orders (" +
                    "Row_ID serial PRIMARY KEY," +
                    "Order_ID VARCHAR(14)," +
                    "Order_Date TIMESTAMP," +
                    "Ship_Date TIMESTAMP," +
                    "Ship_Mode VARCHAR(14)," +
                    "Customer_ID VARCHAR(8)," + 
                    "Customer_Name VARCHAR(22)," +
                    "Segment VARCHAR(11)," +
                    "Country VARCHAR(13)," +
                    "City VARCHAR(16)," +
                    "State VARCHAR(20)," +
                    "Postal_Code VARCHAR(6)," +
                    "Region VARCHAR(7)," +
                    "Product_ID VARCHAR(15)," +
                    "Category VARCHAR(15)," +
                    "Sub_Category VARCHAR(11)," +
                    "Product_Name VARCHAR(127)," +
                    "Sales NUMERIC(9, 4)," +
                    "Quantity NUMERIC(3, 1)," +
                    "Discount NUMERIC(3, 2)," +
                    "Profit NUMERIC(8, 4)" +
                ");";
    
    clientpg.query(pgTable, function(err, results){
        if(err) throw err;
        else{
            console.log("Table Created");
            var flag = 1;
            var offset = 0;
            mysqlConnection.connect(function(err){
                if(err) throw err;
                console.log("Connected!");
                //selama datanya masih ada
                promiseWhile(function () { return flag == 1; }, function () {
                    mysqlConnection.query("SELECT * FROM orders LIMIT "+offset+",1000", function(err,rows,fields){
                        if(err) throw err;
                        else{
                            if(rows.length > 0){
                                const params = [];
                                const chunks = [];
                                rows.forEach(row => {
                                    const valueClause = [];
                                    fields.forEach(field => {
                                        params.push(row[field.orgName]);
                                        valueClause.push('$' + params.length);
                                    })
                                    chunks.push('(' + valueClause.join(', ') + ')');
                                });

                                var pginsert = 'INSERT INTO orders (' + fields.map(field => '"' + field.orgName.toLowerCase() + '"').join(',') + ') ' +
                                            'VALUES ' + chunks.join(', ');
                                try{
                                    pgQuery(clientpg,pginsert,params);
                                }catch(err){
                                    console.error(`Error occured when offset ${offset}, moving on...`);
                                }
                                console.log(rows.length+" Data Inserted");
                            }
                            else{
                                // sudah tidak ada data
                                flag = 0;
                            }
                        }
                    });
                    offset += 1000;
                    return Q.delay(500); // arbitrary async
                }).then(function () {
                    console.log("Done");
                }).done();
            });
        }
    });
    
});
