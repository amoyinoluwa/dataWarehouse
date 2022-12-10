from tkinter import *
from tkinter import messagebox
from mysql.connector import connect
import pandas as pd

"""
A data warehouse interface that converts user inputs into SQL queries
and displays the results in a table.
"""

class DataWarehouse(Tk):
    """
    This is the main class for the Data Warehouse. It creates the main window and
    the widgets that will be used to create the query. It also contains the
    functions that will be used to create the query.
    """
    def __init__(self):
        """
        This is the constructor for the DataWarehouse class.
        """
        Tk.__init__(self)
        self.title("Data Warehouse")
        self.geometry("500x500")
        self.create_widgets()
        self.resizable(2000, 2000)
        self.select = "SELECT st.student_id, student_name"
        self.join = "\nJOIN student as st ON g.student_id = st.student_id"
        self.where = "WHERE"
        self.select_statements = {"degree": "d.degree_name",
                                    "major": "m.major_name",
                                    "status": "s.status_name",
                                    "semester": "sm.semester_name, sm.Month, sm.Day",
                                    "address": "a.address_info",
                                    "college": "c.college_name",
                                    "grad_year": "g.grad_year",
                                    "gpa": "g.gpa"
                                    }
        self.join_statements = {"degree": "\nJOIN degree as d ON g.degree_id = d.degree_id",
                                "major": "\nJOIN major as m ON g.major_id = m.major_id",
                                "status": "\nJOIN status as s ON g.status_id = s.status_id",
                                "semester": "\nJOIN semester as sm ON g.semester_id = sm.semester_id",
                                "address": "\nJOIN address as a ON st.address_id = a.id",
                                "college": "\nJOIN college as c ON c.college_id = m.college_id",
                                "student": "\nJOIN student as st ON g.student_id = st.student_id"
                                }
        self.where_statements = {"degree": "d.degree_name = ",
                                 "major": "m.major_name = ",
                                 "status": "s.status_name = ",
                                 "semester": "sm.semester_name LIKE ",
                                 "student": "st.student_id = ",
                                 "address": "a.address_info = ",
                                 "student": "st.student_name =",
                                 "grad_year": "g.grad_year =",
                                "college": "c.college_name =",
                                "high_gpa": "g.gpa > 3",
                                "low_gpa": "g.gpa < 2",
                                "avg_gpa": "g.gpa > 2 AND g.gpa < 3"
                                 }
    def conn(self):
        """
        This function connects to the database.
        """
        self.db = connect(host = "localhost", user = "root", passwd = "passwd", db = "student_data")
        self.cursor = self.db.cursor()

    def create_widgets(self):
        """
        This function creates the widgets for the main window.
        """
        self.lbl_title = Label(self, text="Student Data Warehouse", font=("Arial", 20))
        self.lbl_title.grid(row=0, column=0, columnspan=2)

        self.rollup_btn = Button(self, text="Click for OLAP operation", command=self.input_values)
        self.rollup_btn.place(relx=0.5, rely=0.5, anchor=CENTER)
        self.rollup_btn.grid(row=3, column=0, sticky="NSEW")
    
    def parseString(self, s):
        """
        This function parses the string to be used in the query.
        
        Parameters:
            s (str): The string to be parsed.
            Returns: The parsed string.  
        """
        if not s:
            return ""
        curr = ", "
        for i in range(len(s)):
            curr += s[i]
            if i != len(s) - 1:
                curr += ', '
        return curr

    def input_values(self):
        """
        This function creates the widgets for the OLAP operation.
        """
        self.conn()
        self.destroy_()
        self.degree_ = Label(self, text="Degree: ", font=("Arial", 15))
        self.degree_.grid(row=0, column=0, sticky="W")
        self.degree_entry = Entry(self)
        self.degree_entry.grid(row=0, column=1)
        self.major_ = Label(self, text="Major: ", font=("Arial", 15))
        self.major_.grid(row=2, column=0, sticky="W")
        self.grad_year = Label(self, text="Graduation Year: ", font=("Arial", 15))
        self.grad_year.grid(row=3, column=0, sticky="W")
        self.student_name = Label(self, text="Student Name: ", font=("Arial", 15))
        self.student_name.grid(row=4, column=0, sticky="W")
        self.student_name_entry = Entry(self)
        self.student_name_entry.grid(row=4, column=1)
        self.college_name = Label(self, text="College Name: ", font=("Arial", 15))
        self.college_name.grid(row=5, column=0, sticky="W")
        self.college_name_entry = Entry(self)
        self.college_name_entry.grid(row=5, column=1)
        self.major_entry = Entry(self)
        self.major_entry.grid(row=2, column=1)
        self.grad_year_entry = Entry(self)
        self.grad_year_entry.grid(row=3, column=1)
        self.text_btn = Button(self, text="Submit", command=self.exec_)
        self.text_btn.grid(row=9, column=0)
        self.status_ = Label(self, text="Status: ", font=("Arial", 15))
        self.status_.grid(row=7, column=0, sticky="W")
        self.status_entry = Entry(self)
        self.status_entry.grid(row=7, column=1)
        self.gpa_ = Label(self, text="GPA: ", font=("Arial", 15))
        self.gpa_.grid(row=8, column=0, sticky="W")
        self.gpa_entry = Entry(self)
        self.gpa_entry.grid(row=8, column=1)
        self.semester_ = Label(self, text="Semester: ", font=("Arial", 15))
        self.semester_.grid(row=6, column=0, sticky="W")
        self.semester_entry = Entry(self)
        self.semester_entry.grid(row=6, column=1)
        self.address_ = Label(self, text="Address: ", font=("Arial", 15))
        self.address_.grid(row=1, column=0, sticky="W")
        self.address_entry = Entry(self)
        self.address_entry.grid(row=1, column=1)

    def exec_(self):
        """
        This function executes the query.
        """
        isWhereInUse = False
        tables = []
        diceStatement = ''
        rollUpStatement = []
        if self.major_entry.get() != '':
            tables.append(self.select_statements["major"])
            diceStatement += "\nMajor: " + self.major_entry.get()
            rollUpStatement.append("Major")
            self.join += self.join_statements["major"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["major"] + '\'' + self.major_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["major"] + '\'' + self.major_entry.get() + '\''
        if self.grad_year_entry.get() != '':
            tables.append(self.select_statements["grad_year"])
            diceStatement + "\nGraduation Year: " + self.grad_year_entry.get()
            rollUpStatement.append("Time to Graduation Year")
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["grad_year"] + self.grad_year_entry.get()
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["grad_year"] + self.grad_year_entry.get()
        if self.degree_entry.get() != '':
            tables.append(self.select_statements["degree"])
            diceStatement += "\nDegree: " + self.degree_entry.get()
            rollUpStatement.append("Degree")
            self.join += self.join_statements["degree"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["degree"] + '\'' + self.degree_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["degree"] + '\'' + self.degree_entry.get() + '\''
        if self.student_name_entry.get() != '':
            tables.append(self.select_statements["student"])
            diceStatement += "\nStudent Name: " + self.student_name_entry.get()
            self.join += self.join_statements["student"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["student"] + '\'' + self.student_name_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["student"] + '\'' + self.student_name_entry.get() + '\''
        if self.college_name_entry.get() != '':
            tables.append(self.select_statements["college"])
            diceStatement += "\nCollege Name: " + self.college_name_entry.get()
            if self.major_entry.get() == '':
                self.join += self.join_statements["major"]
            self.join += self.join_statements["college"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["college"] + '\'' + self.college_name_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["college"] + '\'' + self.college_name_entry.get() + '\''
        if self.status_entry.get() != '':
            tables.append(self.select_statements["status"])
            diceStatement += "\nStatus: " + self.status_entry.get()
            rollUpStatement.append("Status")
            self.join += self.join_statements["status"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["status"] + '\'' + self.status_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["status"] + '\'' + self.status_entry.get() + '\''
        if self.gpa_entry.get() != '':
            tables.append(self.select_statements["gpa"])
            gpa_select = self.gpa_entry.get()
            rollUpStatement.append("GPA")
            if gpa_select.upper() == 'HIGH':
                gpa_select = 'high_gpa'
            elif gpa_select.upper() == 'LOW':
                gpa_select = 'low_gpa'
            else:
                gpa_select = 'avg_gpa'
            diceStatement += "\nGPA: " + self.gpa_entry.get()
            if not isWhereInUse:
                self.where += ' ' + self.where_statements[gpa_select]
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements[gpa_select]
        if self.semester_entry.get() != '':
            tables.append(self.select_statements["semester"])
            diceStatement += "\nSemester: " + self.semester_entry.get()
            rollUpStatement.append("Semester")
            self.join += self.join_statements["semester"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["semester"] + '\'%' + self.semester_entry.get() + '%\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["semester"] + '\'' + self.semester_entry.get() + '\''
        if self.address_entry.get() != '':
            tables.append(self.select_statements["address"])
            diceStatement += "\nAddress: " + self.address_entry.get()
            rollUpStatement.append("Location: Address")
            self.join += self.join_statements["address"]
            if not isWhereInUse:
                self.where += ' ' + self.where_statements["address"] + '\'' + self.address_entry.get() + '\''
                isWhereInUse = True
            else:
                self.where += " AND " + self.where_statements["address"] + '\'' + self.address_entry.get() + '\''
        query = self.select + self.parseString(tables) + "\nFROM graduate_info as g" + self.join + '\n' + self.where
        operation = "C1: Rollup on Student to" + self.parseString(rollUpStatement) + "\nC2: Dice C1 on Student: {}".format(diceStatement)
        self.destroy_()
        try:
            self.result = pd.read_sql(query, self.db)
            pd.set_option('display.max_rows', 110)
            pd.set_option('display.max_columns', 10)
            Label(self, text="{} result(s) were returned".format(len(self.result))).grid(row=0, column=0)
            if len(self.result):
                text = Text(self, height=10)
                text.grid(row=1, column=0)
                #vertical scrollbar
                scrollbar = Scrollbar(self, orient='vertical', command=text.yview)
                scrollbar.grid(row=1, column=1, sticky='ns')
                #horizontal scrollbar
                scrollbar = Scrollbar(self, orient='horizontal', command=text.xview)
                scrollbar.grid(row=2, column=0, sticky='ew')
                #configure text widget
                text.configure(yscrollcommand=scrollbar.set, xscrollcommand=scrollbar.set, wrap='none')
                text.insert(END, self.result)
            Label(self, text=operation).grid(row=4, column=0)
            Label(self, text="SQL query used to get result:", font=("Times", 20)).grid(row=6, column=0)
            Label(self, text=query, wraplength=500, font=("Times", 15)).grid(row=7, column=0)
            self.back_button = Button(self, text="Back", command=self.input_values)
            self.back_button.grid(row=8, column=0)
            self.quit_button = Button(self, text="Quit", command=self.destroy)
            self.quit_button.grid(row=8, column=1)
        except Exception as e:
            messagebox.showerror("Error", e)
            self.destroy()
            print(query) #error handling

    def run(self):
        """
        Runs the mainloop
        """
        self.mainloop()
    def destroy_(self) -> None:
        """
        Destroys all widgets from the screen
        """
        for widget in self.winfo_children():
            widget.destroy()
dw = DataWarehouse()
dw.run()
