package analisisVentas;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int salesOver5000 = 0;
        int sales3000to5000 = 0;
        int salesUnder3000 = 0;
        double totalSales = 0;

        // Procesar cada precio en la lista de valores
        for (Text value : values) {
            double price = Double.parseDouble(value.toString());

            if (price > 5000) {
                salesOver5000++;
            } else if (price >= 3000 && price <= 5000) {
                sales3000to5000++;
            } else if (price < 3000) {
                salesUnder3000++;
            }

            totalSales += price;
        }

        // Obtener el estado del país para la agrupación
        String[] fields = key.toString().split(",");
        String countryState = (fields.length > 2) ? fields[2] : "N/A"; //PRUEBA

        // Emitir resultados
        context.write(new Text("Pais: " + key.toString() + ", State: " + countryState),
                      new Text("Ventas sobre $5000: " + salesOver5000 +
                               ", Ventas entre $3000 y $5000: " + sales3000to5000 +
                               ", Ventas por debajo de $3000: " + salesUnder3000 +
                               ", Total de las ventas: " + totalSales));
    }
}