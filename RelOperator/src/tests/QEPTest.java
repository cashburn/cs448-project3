package tests;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.SortMergeJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;
import java.io.*;
// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {

	public static Schema s_Employee;
	public static Schema s_Department;
	String pathfile;
	

	public static void main(String[] args){
		QEPTest qep = new QEPTest();
		qep.create_minibase();
		qep.pathfile = args[0];
		//Init Employee
		s_Employee = new Schema(5);
		s_Employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_Employee.initField(1, AttrType.STRING, 20, "Name");
		s_Employee.initField(2, AttrType.INTEGER, 4, "Age");
		s_Employee.initField(3, AttrType.INTEGER, 12, "Salary");
		s_Employee.initField(4, AttrType.INTEGER, 4, "DeptId");

		//Init dept
		s_Department = new Schema(4);
		s_Department.initField(0, AttrType.INTEGER, 4, "DeptId");
		s_Department.initField(1, AttrType.STRING, 20, "Name");
		s_Department.initField(2, AttrType.INTEGER, 12, "MinSalary");
		s_Department.initField(3, AttrType.INTEGER, 12, "MaxSalary");

		//init tuples
		HeapFile file = new HeapFile(null);
		HashIndex index = new HashIndex(null);
		HeapFile file2 = new HeapFile(null);
		HashIndex index2 = new HashIndex(null);
		qep.initAll(file, index, file2, index2);
		//qep.qep1(file, index, file2, index2);
		//qep.qep2(file, index, file2, index2);
		//System.out.println("HELLO IT ME");
		
	}


	public void initAll(HeapFile file, HashIndex index, HeapFile file2, HashIndex index2){
		String depPath = pathfile + "/Department.txt";
		String empPath = pathfile + "/Employee.txt";
		String depString = null;
		String empString = null;
		try{
			FileReader fr = new FileReader(depPath);
			FileReader fr2 = new FileReader(empPath);

			BufferedReader br = new BufferedReader(fr);
			BufferedReader br2 = new BufferedReader(fr2);
			String line = null;
			line = br.readLine();//Eat first line, with headers 
			while((line = br.readLine()) != null){
				String[] temp = line.split(", ");
				Tuple t = new Tuple(s_Department);
				t.setIntFld(0, Integer.parseInt(temp[0]));
				t.setStringFld(1, temp[1]);
				t.setIntFld(2, Integer.parseInt(temp[2]));
				t.setIntFld(3, Integer.parseInt(temp[3]));

				RID rid = file.insertRecord(t.getData());
				index.insertEntry(new SearchKey(Integer.parseInt(temp[0])), rid);
			}

			line = null ;
			line = br2.readLine(); //eat first line
			while((line = br2.readLine()) != null){
				String[] temp = line.split(", ");
				Tuple t = new Tuple(s_Employee);
				t.setIntFld(0, Integer.parseInt(temp[0]));
				t.setStringFld(1, temp[1]);
				t.setIntFld(2, Integer.parseInt(temp[2]));
				t.setIntFld(3, Integer.parseInt(temp[3]));
				t.setIntFld(3, Integer.parseInt(temp[4]));

				RID rid = file2.insertRecord(t.getData());
				index2.insertEntry(new SearchKey(Integer.parseInt(temp[0])), rid);
			}

		} catch (Exception e) {

		}
	}

	public void qep1(HeapFile file, HashIndex index, HeapFile file2, HashIndex index2){
		//Display for each employee their ID, Name and age
		FileScan scan = new FileScan(s_Employee, file2);
		Projection pro = new Projection(scan, 0, 1, 2);
		pro.execute();
	}

	public void qep2(HeapFile file, HashIndex index, HeapFile file2, HashIndex index2){
		//Display the Name for the departments with MinSalary = MaxSalary
		FileScan scan = new FileScan(s_Department, file);
		Predicate[] preds = new Predicate[] {
				new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO,
						3)};
		Selection sel = new Selection(scan, preds);
		Projection pro = new Projection(sel, 1);
		pro.execute();
	}

	public void qep3(HeapFile file, HashIndex index, HeapFile file2, HashIndex index2){
		//For each employee, display their Name and the Name of their department as well as the
		//minimum salary of their department
	}

	public void qep4(HeapFile file, HashIndex index, HeapFile file2, HashIndex index2){
		//Display the Name for each employee whose Salary is less than the maximum salary and
		//greater than the minimum salary of their department
	}
}
