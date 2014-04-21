import com.mongodb.*;

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

