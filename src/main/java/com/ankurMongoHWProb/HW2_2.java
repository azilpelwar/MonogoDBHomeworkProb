package com.ankurMongoHWProb;

import com.mongodb.*;
/*
* Problem:
* Write a program in the language of your choice that will remove the grade of type "homework" with the lowest score for each student from the dataset that you imported in HW 2.1. Since each document is one grade, it should remove one document per student.

Hint/spoiler: If you select homework grade-documents, sort by student and then by score, you can iterate through and find the lowest score for each student by noticing a change in student id. As you notice that change of student_id, remove the document.

To confirm you are on the right track, here are some queries to run after you process the data with the correct answer shown:

Let us count the number of grades we have:
> db.grades.count()
600
Now let us find the student who holds the 101st best grade across all grades:
> db.grades.find().sort({'score':-1}).skip(100).limit(1)
{ "_id" : ObjectId("50906d7fa3c412bb040eb709"), "student_id" : 100, "type" : "homework", "score" : 88.50425479139126 }
Now let us sort the students by student_id, score and see what the top five docs are:
> db.grades.find({},{'student_id':1, 'type':1, 'score':1, '_id':0}).sort({'student_id':1, 'score':1, }).limit(5)
{ "student_id" : 0, "type" : "quiz", "score" : 31.95004496742112 }
{ "student_id" : 0, "type" : "exam", "score" : 54.6535436362647 }
{ "student_id" : 0, "type" : "homework", "score" : 63.98402553675503 }
{ "student_id" : 1, "type" : "homework", "score" : 44.31667452616328 }
{ "student_id" : 1, "type" : "exam", "score" : 74.20010837299897 }
To verify that you have completed this task correctly, provide the identity of the student with the highest average in the class with following query that uses the aggregation framework. The answer will appear in the _id field of the resulting document.

> db.grades.aggregate({'$group':{'_id':'$student_id', 'average':{$avg:'$score'}}}, {'$sort':{'average':-1}}, {'$limit':1})
*
* */

public class HW2_2 {
    public static void main(String[] args) {
        try {
            MongoClient client = new MongoClient();
            DB db = client.getDB("students");
            DBCollection collection = db.getCollection("grades");

            DBCursor cur = collection.find(new BasicDBObject("type", "homework"))
                    .sort(new BasicDBObject("student_id", 1).append("score",1));

            //the documents are sorted in ascending order according to student_id and then score.
            //Each student id's first document has to be removed(since it is with the lowest score)
            //Whenever there is a change in the student id, we have to remove the new student id's document

            int student_id = -1;
            int count = 0;

            if (cur.hasNext()) {
                for (DBObject obj = cur.next(); cur.hasNext(); obj = cur.next()) {

                    if ((Integer)obj.get("student_id") != student_id) {

                        removedoc(obj, collection);
                        count++;
                    }
                    student_id = (Integer) obj.get("student_id");
                }
                System.out.println("Total records deleted:" + count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {

        }
    }

    static void removedoc(DBObject obj_to_del, DBCollection coll) {
        coll.remove(obj_to_del);
        System.out.println("Removing:" + obj_to_del.get("student_id") + " wih score " + obj_to_del.get("score"));
    }
}

