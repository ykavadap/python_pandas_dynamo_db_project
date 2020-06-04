import boto3
import pandas as pd

dynamodb=boto3.resource('dynamodb')

### Scanning 'Department' table
department_table=dynamodb.Table('Department')
department_table=department_table.scan()

department_table=department_table['Items']
#print(department_table)
department_table=pd.DataFrame.from_dict(department_table,orient='columns')
#print(department_table)
department_table['Dept_no']=pd.to_numeric(department_table['Dept_no']).astype(int)
#print(department_table.dtypes)

### Scanning 'Employee_Info' table
employee_info_table=dynamodb.Table('Employee_Info')
employee_info_table=employee_info_table.scan()
employee_info_table=employee_info_table['Items']
employee_info_table=pd.DataFrame.from_dict(employee_info_table,orient='columns')
#print(employee_info_table.dtypes)
employee_info_table['Emp_ID']=pd.to_numeric(employee_info_table['Emp_ID']).astype(int)
# print(employee_info_table.dtypes)

### Scanning 'Contact_Information' table
contact_info_table=dynamodb.Table('Contact_Information')
contact_info_table=contact_info_table.scan()
contact_info_table=contact_info_table['Items']
contact_info_table=pd.DataFrame.from_dict(contact_info_table,orient='columns')
contact_info_table[['Emp_ID']]=contact_info_table[['Emp_ID']].apply(pd.to_numeric).astype(int)
#print(contact_info_table.dtypes)

### Scanning 'Employee_Status' table
employee_status_table=dynamodb.Table('Employee_Status')
employee_status_table=employee_status_table.scan()
employee_status_table=employee_status_table['Items']
employee_status_table=pd.DataFrame.from_dict(employee_status_table,orient='columns')
employee_status_table[['Dept_no','Emp_ID']]=employee_status_table[['Dept_no','Emp_ID']].apply(pd.to_numeric).astype(int)

### Scanning 'Location_Details' table
location_details_table=dynamodb.Table('Location_Details')
location_details_table=location_details_table.scan()
location_details_table=location_details_table['Items']
location_details_table=pd.DataFrame.from_dict(location_details_table,orient='columns')
location_details_table['Emp_ID']=pd.to_numeric(location_details_table['Emp_ID']).astype(int)

### Scanning 'Salaries' table
salaries_table=dynamodb.Table('Salaries')
salaries_table=salaries_table.scan()
salaries_table=salaries_table['Items']
salaries_table=pd.DataFrame.from_dict(salaries_table,orient='columns')
salaries_table[['Current_sal','Previous_Year_sal']]=salaries_table[['Current_sal','Previous_Year_sal']].apply(pd.to_numeric)
salaries_table[['Dept_no','Emp_ID']]=salaries_table[['Dept_no','Emp_ID']].apply(pd.to_numeric).astype(int)
#print(salaries_table.dtypes)

### Find the sum of Current salaries of all 'Information Technology' department employees.
### Query which provides aggregate(sum) data

info_tech_cur_sal=pd.merge(department_table,salaries_table,how='outer',on=['Dept_no'])
info_tech_cur_sal=info_tech_cur_sal.loc[info_tech_cur_sal['Dept_name']=='Information Technology']
#print(info_tech_cur_sal)
info_tech_cur_sal_total=info_tech_cur_sal['Current_sal'].sum()
#print(info_tech_cur_sal_total)

### Find the department head of the employee 'Yashaswi'
### Spanning multiple(3) tables

dept_head=pd.merge(employee_info_table,employee_status_table,how='outer',on=['Emp_ID'])
dept_head=pd.merge(dept_head,department_table,how='outer',on=['Dept_no'])
dept_head=dept_head.loc[dept_head['First_name']=='Yashaswi']
dept_head=dept_head['Dept_head']
#print(dept_head)

### Find all the employees first and last names who work in the state of 'Michigan'

mi_employees=pd.merge(employee_info_table,location_details_table,how='outer',on=['Emp_ID'])
mi_employees=mi_employees.loc[mi_employees['Work_State']=='MI']
mi_employees=mi_employees[['First_name','Last_name']]

#print(mi_employees)

### Find Nikhils personal email and residential phone number.

email_phone=pd.merge(employee_info_table,contact_info_table,how='outer',on=['Emp_ID'])
email_phone=email_phone.loc[email_phone['First_name']=='Nikhil']
email_phone=email_phone[['Personal_email','Home_phone']]
print(email_phone)


