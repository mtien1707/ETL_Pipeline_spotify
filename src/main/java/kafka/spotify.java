package kafka;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvDate;
import lombok.Getter;
import lombok.ToString;

import java.util.Date;

@Getter
@ToString
public class spotify {

    @CsvBindByName(column = "title")
    String title;

    @CsvBindByName(column = "rank")
    Long rank;

    @CsvBindByName(column = "artist")
    @CsvDate("yyyy-MM-dd")
    Date date;

    @CsvBindByName(column = "url")
    String url;

    @CsvBindByName(column = "chart")
    String chart;

    @CsvBindByName(column = "trend")
    String trend;

    @CsvBindByName(column = "streams")
    String streams;
}
