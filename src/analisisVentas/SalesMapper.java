package analisisVentas;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String countryFilter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Obtener el país filtrado desde la configuración
        countryFilter = context.getConfiguration().get("countryFilter");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parsear la línea del dataset
        String[] fields = value.toString().split(",");

        // Extraer información necesaria
        String country = fields[7];
        String priceStr = fields[2];

        // Filtrar por país antes de emitir clave-valor
        if (country.equals(countryFilter)) {
            context.write(new Text(country), new Text(priceStr));
        }
    }
}

