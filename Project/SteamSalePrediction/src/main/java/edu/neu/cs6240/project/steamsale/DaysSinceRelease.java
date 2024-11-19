package edu.neu.cs6240.project.steamsale;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DaysSinceRelease extends EvalFunc<Integer> {
    public Integer exec(Tuple input) throws ExecException {
      if (input == null || input.size() == 0) {
        return null;
      }
        
      try {
        String dateStr = (String) input.get(0);
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy");
        Date releaseDate = null;
        releaseDate = sdf.parse(parseAndValidateDate(dateStr));
        long millis = System.currentTimeMillis() - releaseDate.getTime();
        return (int) (millis / (1000 * 60 * 60 * 24));  // Convert milliseconds to days
      } catch (ParseException e) {
        e.printStackTrace();
      }
      return 0;
    }

    private String parseAndValidateDate(String date){
      String[] dateParts = date.split(" ");
      if(dateParts.length==3){
        return date;
      }
      return dateParts[0]+" 15, "+dateParts[1];
    }


}