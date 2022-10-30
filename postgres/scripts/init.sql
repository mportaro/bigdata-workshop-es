CREATE SCHEMA workshop;

DROP TABLE IF EXISTS churn;
CREATE TABLE churn (
  CLIENTNUM integer NOT NULL,
  Attrition_Flag integer NOT NULL,
  Customer_Age integer NOT NULL,
  Gender varchar(5) NOT NULL,
  Dependent_count integer	NOT NULL,
  Education_Level varchar(15)	NOT NULL,
  Marital_Status varchar(10)	NOT NULL,
  Income_Category varchar(20)	NOT NULL,
  Card_Category varchar(10)	NOT NULL,
  Months_on_book integer	NOT NULL,
  Total_Relationship_Count integer	NOT NULL,
  Months_Inactive_12_mon integer	NOT NULL,
  Contacts_Count_12_mon integer	NOT NULL,
  Total_Revolving_Bal integer	NOT NULL,
  Total_Amt_Chng_Q4_Q1 integer	NOT NULL,
  Total_Trans_Ct integer	NOT NULL,
  train boolean	NOT NULL,
  Credit_Limit double precision	NOT NULL,
  Avg_Open_To_Buy double precision	NOT NULL,
  Total_Trans_Amt double precision	NOT NULL,
  Total_Ct_Chng_Q4_Q1 double precision	NOT NULL,
  Avg_Utilization_Ratio double precision	NOT NULL,
  PRIMARY KEY(CLIENTNUM)
);
