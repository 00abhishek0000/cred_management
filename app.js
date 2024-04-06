import express from 'express';
import mysql from 'mysql2';
import axios from 'axios';
require('dotenv').config()
const app = express();
const port = 3000;

app.use(express.json())


const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE
});

connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL database: ', err);
        return;
    }
    console.log('Connected to MySQL database');
});

// REGISTER ENDPOINT
app.post('/register', (req, res) => {
    const { first_name, last_name, age, monthly_salary, phone_number } = req.body;

    if (!first_name || !last_name || !age || !monthly_salary|| !phone_number) {
        return res.status(400).json({ error: 'Missing required fields' });
    }

    // Calculate approved limit based on monthly income
    const approved_limit = Math.round(36 * monthly_salary / 100000) * 100000; // Round to nearest lakh

    // add to database
    connection.query(
        'INSERT INTO customers (customer_id, first_name, last_name, monthly_salary, approved_limit, phone_number) SELECT COALESCE(MAX(customer_id), 0) + 1, ?, ?, ?, ?, ? FROM customers',
        [first_name, last_name, monthly_salary, approved_limit, phone_number],
        (err, result) => {
            if (err) {
                console.error('Error inserting new customer into database: ', err);
                return res.status(500).json({ error: 'Internal server error' });
            }

            const newCustomerId = result.insertId;
            const debt = 0;
            console.log('New customer added to database with ID: ', newCustomerId);

            const newCustomer = {
                customer_id: newCustomerId,
                first_name,
                last_name,
                monthly_salary,
                approved_limit,
                phone_number,
                current_debt: debt
            };
            res.status(201).json(newCustomer);
        }
    );
});

// CHECK-ELIGIBILITY ENDPOINT
function calculateCreditScore() {
    // add functionality
}

function calculateEMI(loanAmount, interestRate, tenure) {
    const monthlyInterestRate = (interestRate / 100) / 12;
    const numberOfPayments = tenure;
    const monthlyEMI = loanAmount * monthlyInterestRate * Math.pow(1 + monthlyInterestRate, numberOfPayments) / (Math.pow(1 + monthlyInterestRate, numberOfPayments) - 1);
    return monthlyEMI;
}

app.post('/check-eligibility',(req,res)=>{
    const {customer_id,loan_amount,interest_rate,tenure} = req.body;

    // make sure sum of EMIs < 50% of monthly salary
    const querySalary = `SELECT monthly_salary FROM customers WHERE customer_id = ${customer_id}`;
    let monthly_salary = 0;
    let approval = true;
    let corrected_interest_rate = interest_rate;
    connection.query(querySalary, (err, salaryResult) => {
        if (err) {
          console.error('Error fetching customer salary:', err);
          res.status(500).send('Internal server error');
          return;
        }
        monthly_salary = salaryResult;
      });

    const queryMonthlyPayments = `SELECT SUM(monthly_payment) AS total_monthly_payments FROM loans WHERE customer_id = ${customer_id}`;
    connection.query(queryMonthlyPayments, (err, paymentResult) => {
    if (err) {
        console.error('Error fetching total monthly payments:', err);
        res.status(500).send('Internal server error');
        return;
    }
    const totalMonthlyPayments = paymentResult || 0;
    const maxAllowedPayments = monthly_salary * 0.5;
    // Determine loan eligibility based on monthly payments
    approval = totalMonthlyPayments <= maxAllowedPayments;
    });

    // calculate credit score
    const creditScore = calculateCreditScore(customer_id);
    // check the monthly value to be paid
    const monthlyEMI = calculateEMI(loan_amount, interest_rate, tenure);
    // check the rate of interest and eligibility
    if(creditScore>30 && creditScore<50){
        corrected_interest_rate = Math.max(interest_rate, 12);
    }else if(creditScore>10 && creditScore<30){
        corrected_interest_rate = Math.max(interest_rate, 16);
    }else{
        approval = false;
    }


    const response = {
        customer_id: customer_id,
        approval: approval, // Determine loan approval status based on credit score and other conditions
        interest_rate: interest_rate, // Interest rate on loan
        corrected_interest_rate: corrected_interest_rate, // Corrected interest rate based on credit score
        tenure: tenure, // Tenure of loan
        monthly_installment: monthlyEMI // Monthly installment for repayment
    };
    res.json(response);
});

// CREATE-LOAN ENDPOINT
app.post('/create-loan',async (req,res)=>{
    const { customer_id, loan_amount, interest_rate, tenure } = req.body;
    // Validate request data
    if (!customer_id || !loan_amount || !interest_rate || !tenure) {
        return res.status(400).json({ error: 'Missing required fields' });
    }
    try {
        // Make a request to the /check-eligibility endpoint
        const eligibilityResponse = await axios.post('http://localhost:3000/check-eligibility', {
            customer_id,
            loan_amount,
            interest_rate,
            tenure
        });

        // Process the eligibility response
        const { customer_id,loan_approved,monthly_installment } = eligibilityResponse.data;
        let message = "loan not approved";
        // doubt regarding loan id generation
        let loan_id = 1234
        res.status(200).json({loan_id, customer_id, loan_approved, message, monthly_installment });
    } catch (error) {
        console.error('Error while checking eligibility:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }

    // functionality remaining to add the new loan in database
});

// VIEW LOAN ENDPOINT
app.get('/view-loan/:loan_id', (req, res) => {
    const loan_id = req.params.loan_id;
    // Query to retrieve loan details and customer details
    const query = `
      SELECT loans.loan_id, customers.customer_id AS customer_id, customers.first_name, customers.last_name, customers.phone_number, customers.age,
      loans.loan_amount, loans.interest_rate, loans.monthly_payment, loans.tenure
      FROM loans
      INNER JOIN customers ON loans.customer_id = customers.customer_id
      WHERE loans.loan_id = ?`;

    // Execute the query
    connection.query(query, [loan_id], (error, results) => {
      if (error) {
        console.error('Error retrieving loan details:', error);
        res.status(500).json({ error: 'Internal server error' });
        return;
      }

      // Check if loan with the given loan_id exists
      if (results.length === 0) {
        res.status(404).json({ error: 'Loan not found' });
        return;
      }

      // Construct the response JSON
      const loanDetails = results[0];
      const response = {
        loan_id: loanDetails.loan_id,
        customer: {
          id: loanDetails.customer_id,
          first_name: loanDetails.first_name,
          last_name: loanDetails.last_name,
          phone_number: loanDetails.phone_number,
          age: loanDetails.age
        },
        loan_amount: loanDetails.loan_amount,
        interest_rate: loanDetails.interest_rate,
        monthly_installment: loanDetails.monthly_payment,
        tenure: loanDetails.tenure
      };

      res.json(response);
    });
  });

// MAKE PAYMENT ENDPOINT
app.post('/make-payment/:customer_id/:loan_id', (req, res) => {
    const customer_id = req.params.customer_id;
    const loan_id = req.params.loan_id;
    const paymentAmount = req.body.payment_amount; // Assuming payment_amount is provided in the request body

    // Query to retrieve loan details
    const loanQuery = 'SELECT * FROM loans WHERE customer_id = ? AND loan_id = ?';

    // Execute the loan query
    connection.query(loanQuery, [customer_id, loan_id], (error, results, fields) => {
      if (error) {
        console.error('Error retrieving loan details:', error);
        res.status(500).json({ error: 'Internal server error' });
        return;
      }

      // Check if loan with the given customer_id and loan_id exists
      if (results.length === 0) {
        res.status(404).json({ error: 'Loan not found' });
        return;
      }
      //
      //
      // IMPLEMENT THE LOGIC CAREFULLY
      const loan = results[0];
    //   const outstandingBalance = loan.outstanding_balance;
      const outstandingBalance = loan.loan_amount - (loan.emis_paid_on_time)*loan.monthly_payment;
      const monthlyInstallment = loan.monthly_payment;

      let newOutstandingBalance = outstandingBalance - paymentAmount;
      let remainingEMIs = Math.ceil(newOutstandingBalance / monthlyInstallment);

      // Check if the payment amount is less than, equal to, or more than the due installment amount
      if (paymentAmount < monthlyInstallment) {
        res.status(400).json({ error: 'Payment amount cannot be less than the monthly installment' });
        return;
      } else if (paymentAmount > monthlyInstallment) {
        // Recalculate the remaining EMIs based on the excess payment
        remainingEMIs = Math.ceil(newOutstandingBalance / monthlyInstallment);
      }

      // Update the loan record in the database with the new outstanding balance and remaining EMIs
      const updateQuery = 'UPDATE loans SET outstanding_balance = ?, remaining_emis = ? WHERE customer_id = ? AND loan_id = ?';
      connection.query(updateQuery, [newOutstandingBalance, remainingEMIs, customer_id, loan_id], (error, results, fields) => {
        if (error) {
          console.error('Error updating loan record:', error);
          res.status(500).json({ error: 'Internal server error' });
          return;
        }

        // Construct the response JSON
        const response = {
          customer_id: customer_id,
          loan_id: loan_id,
          payment_amount: paymentAmount,
          new_outstanding_balance: newOutstandingBalance,
          remaining_emis: remainingEMIs
        };

        // Send the response back to the client
        res.json(response);
      });
    });
  });



// VIEW STATEMENT ENDPOINT
app.get('/view-statement/:customer_id/:loan_id', (req, res) => {
    const customer_id = req.params.customer_id;
    const loan_id = req.params.loan_id;

    // Query to retrieve loan details
    const loanQuery = 'SELECT * FROM loans WHERE customer_id = ? AND loan_id = ?';

    // Execute the loan query
    connection.query(loanQuery, [customer_id, loan_id], (error, results, fields) => {
      if (error) {
        console.error('Error retrieving loan details:', error);
        res.status(500).json({ error: 'Internal server error' });
        return;
      }

      // Check if loan with the given customer_id and loan_id exists
      if (results.length === 0) {
        res.status(404).json({ error: 'Loan not found' });
        return;
      }

      const loan = results[0];
      const principal = loan.loan_amount;
      const interestRate = loan.interest_rate;
      const monthlyInstallment = loan.monthly_payment;
      const repaymentsLeft = loan.tenure - loan.emis_paid_on_time;

      // Construct the response JSON
      const response = {
        customer_id: customer_id,
        loan_id: loan_id,
        principal: principal,
        interest_rate: interestRate,
        amount_paid: amountPaid,
        monthly_installment: monthlyInstallment,
        repayments_left: repaymentsLeft
      };

      // Send the response back to the client
      res.json(response);
    });
  });


app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});


// I have a nofe js and express project consistng of soe api endpoints an dconnectee to ,mysql locally,
// Also I have defined celery tasks of inserting values from th e/xlsx files to the same database of mysql.

// I want to dockerise the whole application, how to ddo so ?