package com.ankurMongoHWProb;

/*
* Problem:
*
* Download the students.json file from the Download Handout link and import it into your local Mongo instance with this command:
$ mongoimport -d school -c students < students.json
This dataset holds the same type of data as last week's grade collection, but it's modeled differently. You might want to start by inspecting it in the Mongo shell.

Write a program in the language of your choice that will remove the lowest homework score for each student. Since there is a single document for each student containing an array of scores, you will need to update the scores array and remove the homework.

Hint/spoiler: With the new schema, this problem is a lot harder and that is sort of the point. One way is to find the lowest homework in code and then update the scores array with the low homework pruned. If you are struggling with the Node.js side of this, look at the .splice() operator, which can remove an element from an array in-place.

To confirm you are on the right track, here are some queries to run after you process the data with the correct answer shown:

Let us count the number of students we have:

> use school
> db.students.count()
200
Let's see what Demarcus Audette's record looks like:
> db.students.find({_id:100}).pretty()
{
	"_id" : 100,
	"name" : "Demarcus Audette",
	"scores" : [
		{
			"score" : 47.42608580155614,
			"type" : "exam"
		},
		{
			"score" : 44.83416623719906,
			"type" : "quiz"
		},
		{
			"score" : 39.01726616178844,
			"type" : "homework"
		}
	]
}

To verify that you have completed this task correctly, provide the identify of the student with the highest average in the class with following query that uses the aggregation framework. The answer will appear in the _id field of the resulting document.

> db.students.aggregate({'$unwind':'$scores'},{'$group':{'_id':'$_id', 'average':{$avg:'$scores.score'}}}, {'$sort':{'average':-1}}, {'$limit':1})
* */

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.List;

public class HW3_1 {
    public static void main(String[] args) throws UnknownHostException {
        MongoClient client=new MongoClient();
        DBCollection collection=client.getDB("school").getCollection("students");

        DBCursor cur= collection.find();
        DBObject lowestScore;
        DBObject ModifiedScore;
        for(DBObject obj=cur.next();cur.hasNext();obj=cur.next())
        {
            System.out.println("Scanning student: "+obj.get("_id")+"...");
            List<DBObject> scores_obj= (List<DBObject>) obj.get("scores");

            lowestScore=findLowestHW(scores_obj);

            System.out.println("The lowest score is: "+lowestScore);

            ModifiedScore=deleteLowestHWScore(scores_obj,lowestScore);

            collection.update(new BasicDBObject("_id",obj.get("_id")),
                            new BasicDBObject("$set",
                            new BasicDBObject("scores",ModifiedScore)));

            System.out.println("Student data updated!!");
            System.out.println("Updated record:"+obj);
        }
    }

    private static DBObject findLowestHW(List<DBObject> obj) {

        Double score= Double.valueOf(101);
        DBObject lowest_score=new BasicDBObject();
        for(int i=0;i<obj.size();i++)
        {
            DBObject score_stud=obj.get(i);
            if("homework".equals((String)score_stud.get("type")) && score > (Double)score_stud.get("score"))
            {
                score=(Double)score_stud.get("score");
                lowest_score=score_stud;
            }
        }
        return lowest_score;
    }

    private static DBObject deleteLowestHWScore(List<DBObject> obj, DBObject lowest_score) {

        for(int i=0;i<obj.size();i++)
        {
            DBObject item=obj.get(i);
            if(item.equals(lowest_score)) {
                System.out.println("Document to be removed:"+item);
                obj.remove(obj.indexOf(item));
            }
        }
        return (DBObject)obj;
    }
}
