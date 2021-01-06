
var fs = require('fs');
 var csv = require('fast-csv');
//var csv = require('csv-parser');
const pool = require('./pgdb');

pool.connect(function(err){
    if(err)
    {
        console.log(err);
    }
});

let counter = 0; 

// const results = [];
// fs.createReadStream('5m Sales Records.csv')
//   .pipe(csv())
//   .on('data', (data) => results.push(data))
//   .on('end', () => {
//     console.log(results);
    
//   });

let csvStream = csv.fromPath(".\\csv\\5m Sales Records.csv", { headers: true })
    .on("data", function(record){
        csvStream.pause();

        if(counter < 100)
        {
            let Region = record.Region;
            let Country = record.Country;
            let Item_Type = record.Item_Type;
            let Sales_Channel = record.Sales_Channel;
            let Order_Priority = record.Order_Priority;
            let Order_Date = record.Order_Date;
            let Order_ID = record.Order_ID;
            let Ship_Date = record.Ship_Date;
            let Units_Sold = record.Units_Sold;
            let Unit_Price = record.Unit_Price;
            let Unit_Cost = record.Unit_Cost;
            let Total_Revenue = record.Total_Revenue;
            let Total_Cost = record.Total_Cost;
            let Total_Profit = record.Total_Profit;
//make order_id as primary key
            pool.query("INSERT INTO FL_insurance_sample(Order_ID, Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date,Ship_Date,Units_Sold,Unit_Price,Unit_Cost,Total_Revenue,Total_Cost,Total_Profit) \
            VALUES($1, $2, $3, $4, $5, $6, $7,$8, $9, $10, $11, $12, $13, $14)", [Order_ID, Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date,Ship_Date,Units_Sold,Unit_Price,Unit_Cost,Total_Revenue,Total_Cost,Total_Profit], function(err){
                if(err)
                {
                    console.log(err);
                }
            });
            ++counter;
        }


        csvStream.resume();

    }).on("end", function(){
        console.log("Job is done!");
    }).on("error", function(err){
        console.log(err);
    });