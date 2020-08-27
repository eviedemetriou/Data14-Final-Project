# Data14-Final-Project

Sprint 0:
Goal: --
Accomplished:
- Assigned roles for devs, testers and BAs
- Had a look at the data to make sure we understood what it was
- Wrote the first user stories for the task
- Created an ERD


Sprint 1:
Goal: To clean the data in preparation for putting it into a database
Accomplished:
- Reconfigured access keys to import the data from within S3
Comments:
- Got off to a slow start; most struggled with getting S3 to work so didn't make much progress elsewhere


Sprint 2:
Goal: To have a class for cleaning each of the data files in S3
Accomplished:
- Created a class to extract the data for each file type in S3
Comments:


Sprint 4:
Goal: Complete all 4 classes for the cleaning of files and get started on the new class for building the database
Accomplished: 
- Completed methods for the cleaning classes
- Made a main.py file that calls all cleaning classes and outputs 4 separate dataframes with clean data
- Got started on the class for building database
Comments:
- Will have to change format of text files with flagged data to make it clear what they show
- Tests are not entirely finished so none of the cleaning classes was passed onto dev
