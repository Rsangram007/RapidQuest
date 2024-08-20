const express = require('express');
require('dotenv').config();
const { MongoClient } = require('mongodb');
const cors = require('cors');
const app = express();
const port = 5000;

// MongoDB connection string
 
const client = new MongoClient(process.env.MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true });

console.log(process.env.MONGODB_URI)

async function connectDB() {
   try {
      await client.connect();
      console.log('Connected to MongoDB');
   } catch (error) {
      console.error('Failed to connect to MongoDB', error);
   }
}

connectDB();
app.use(cors());
// Define API endpoints here...
 
app.get('/total-sales', async (req, res) => {
  const { interval } = req.query;

  let groupBy;
  switch (interval) {
    case 'daily':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } },
        day: { $dayOfMonth: { $toDate: '$created_at' } }
      };
      break;
    case 'monthly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } }
      };
      break;
    case 'quarterly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        quarter: {
          $ceil: { $divide: [{ $month: { $toDate: '$created_at' } }, 3] }
        }
      };
      break;
    case 'yearly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } }
      };
      break;
    default:
      return res.status(400).send('Invalid interval specified');
  }

  try {
    const db = client.db('RQ_Analytics');
    const orders = db.collection('shopifyOrders');

    const totalSales = await orders.aggregate([
      {
        $group: {
          _id: groupBy,
          totalSales: { $sum: { $toDouble: '$total_price_set.shop_money.amount' } } // Convert to double
        }
      },
      { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.quarter': 1 } }  // Adjust sorting based on interval
    ]).toArray();

    res.json(totalSales);
  } catch (err) {
    res.status(500).send('Error fetching total sales: ' + err.message);
  }
});


app.get('/sales-growth', async (req, res) => {
  const { interval } = req.query;

  let groupBy;
  let sortBy = {};
  switch (interval) {
    case 'daily':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } },
        day: { $dayOfMonth: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1, '_id.day': 1 };
      break;
    case 'monthly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1 };
      break;
    case 'quarterly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        quarter: {
          $ceil: { $divide: [{ $month: { $toDate: '$created_at' } }, 3] }
        }
      };
      sortBy = { '_id.year': 1, '_id.quarter': 1 };
      break;
    case 'yearly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1 };
      break;
    default:
      return res.status(400).send('Invalid interval specified');
  }

  try {
    const db = client.db('RQ_Analytics');
    const orders = db.collection('shopifyOrders');

    // Step 1: Calculate Total Sales for Each Period
    const totalSales = await orders.aggregate([
      {
        $group: {
          _id: groupBy,
          totalSales: { $sum: { $toDouble: '$total_price_set.shop_money.amount' } } // Ensure amount is a number
        }
      },
      { $sort: sortBy }  // Adjust sorting based on interval
    ]).toArray();

    // Step 2: Calculate Growth Rate
    let previousTotalSales = null;
    const salesGrowth = totalSales.map((current, index) => {
      const currentTotalSales = current.totalSales;
      const growthRate = previousTotalSales !== null
        ? ((currentTotalSales - previousTotalSales) / previousTotalSales) * 100
        : 0;  // Return 0% growth for the first period
      previousTotalSales = currentTotalSales;

      return {
        period: current._id,
        totalSales: currentTotalSales,
        growthRate: growthRate
      };
    });

    res.json(salesGrowth);
  } catch (err) {
    res.status(500).send('Error fetching sales growth: ' + err.message);
  }
});

 
 
app.get('/new-customers', async (req, res) => {
  const { interval } = req.query;

  let groupBy;
  let sortBy = {};
  switch (interval) {
    case 'daily':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } },
        day: { $dayOfMonth: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1, '_id.day': 1 };
      break;
    case 'monthly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1 };
      break;
    case 'quarterly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        quarter: { $ceil: { $divide: [{ $month: { $toDate: '$created_at' } }, 3] } }
      };
      sortBy = { '_id.year': 1, '_id.quarter': 1 };
      break;
    case 'yearly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1 };
      break;
    default:
      return res.status(400).send('Invalid interval specified');
  }

  try {
    const db = client.db('RQ_Analytics');
    const customers = db.collection('shopifyCustomers');

    // Aggregate pipeline to group and count new customers by the specified interval
    const newCustomers = await customers.aggregate([
      {
        $group: {
          _id: groupBy,
          count: { $sum: 1 }  // Count the number of new customers in each group
        }
      },
      { $sort: sortBy }  // Sort the results based on the interval
    ]).toArray();

    res.json(newCustomers);
  } catch (err) {
    res.status(500).send('Error fetching new customers: ' + err.message);
  }
});


app.get('/repeat-customers', async (req, res) => {
  const { interval } = req.query;

  let groupBy;
  let sortBy = {};
  switch (interval) {
    case 'daily':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } },
        day: { $dayOfMonth: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1, '_id.day': 1 };
      break;
    case 'monthly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        month: { $month: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1, '_id.month': 1 };
      break;
    case 'quarterly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } },
        quarter: { $ceil: { $divide: [{ $month: { $toDate: '$created_at' } }, 3] } }
      };
      sortBy = { '_id.year': 1, '_id.quarter': 1 };
      break;
    case 'yearly':
      groupBy = {
        year: { $year: { $toDate: '$created_at' } }
      };
      sortBy = { '_id.year': 1 };
      break;
    default:
      return res.status(400).send('Invalid interval specified');
  }

  try {
    const db = client.db('RQ_Analytics');
    const orders = db.collection('shopifyOrders');

    // Step 1: Group by customer and time period, and count purchases
    const customerPurchases = await orders.aggregate([
      {
        $project: {
          customer_id: 1,
          created_at: { $toDate: '$created_at' }  // Ensure created_at is a date
        }
      },
      {
        $group: {
          _id: {
            customer_id: '$customer_id',
            period: groupBy
          },
          count: { $sum: 1 }  // Count the number of purchases for each customer in the period
        }
      },
      {
        $group: {
          _id: '$_id.period',
          repeatCustomers: {
            $sum: {
              $cond: [{ $gt: ['$count', 1] }, 1, 0]  // Only count if purchases > 1
            }
          }
        }
      },
      { $sort: sortBy }  // Sort based on the interval
    ]).toArray();

    res.json(customerPurchases);
  } catch (err) {
    res.status(500).send('Error fetching repeat customers: ' + err.message);
  }
});




app.get('/customer-ltv-cohorts', async (req, res) => {
   try {
     const db = client.db('RQ_Analytics');
     const orders = db.collection('shopifyOrders');
 
     const ltvCohorts = await orders.aggregate([
       {
         $group: {
           _id: {
             customer_id: '$customer_id',
             cohort: {
               $dateToString: {
                 format: '%Y-%m',
                 date: { $toDate: '$created_at' }  // Ensure this field is treated as a date
               }
             }
           },
           lifetimeValue: { $sum: '$total_price_set.shop_money.amount' }
         }
       },
       {
         $group: {
           _id: '$_id.cohort',
           avgLTV: { $avg: '$lifetimeValue' }
         }
       },
       { $sort: { '_id': 1 } }
     ]).toArray();
 
     res.json(ltvCohorts);
   } catch (err) {
     res.status(500).send('Error fetching customer lifetime value by cohorts: ' + err.message);
   }
 });
 





app.get('/geographical-distribution', async (req, res) => {
  try {
    const db = client.db('RQ_Analytics');
    const customers = db.collection('shopifyCustomers');

    const geoDistribution = await customers.aggregate([
      { $group: { _id: '$default_address.city', customerCount: { $sum: 1 } } },
      { $sort: { customerCount: -1 } }
    ]).toArray();

    res.json(geoDistribution);
  } catch (err) {
    res.status(500).send('Error fetching geographical distribution: ' + err.message);
  }
});


app.listen(port, () => {
   console.log(`Server is running on http://localhost:${port}`);
});
