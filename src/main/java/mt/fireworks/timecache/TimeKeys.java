package mt.fireworks.timecache;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * Enkodiranje ključeva za keš. Ključ sadrži tstamp podatka i njegov index.
 * Tstamp je u ključu pohranjen kao 29 bitni offset u sekundama od
 * početka UTC godine u kojoj je kod pokrenut. Milisekundni dio se gubi u
 * procesu kodiranja.
 *
 * Po dizajnu maksimalni offset je ~10 godina. Kod nije stabilan za
 * pohranu tstampova koji su više od 10 godina u budučnosti.
 *
 * Index je pohranjen do 35 bitova. U slučaju korištenja indexa većeg od 2^35-1,
 * kod baca grešku.
 */
public class TimeKeys {

    final long epoch = startingYear();
    final long maxIndex = (1l << 35) - 1l; // 34_359_738_367
    final long mask = BitsAndBytes.lmask(35);


    /** @return tstamp of current new year, unless if current month is January. Than it is last new year */
    long startingYear() {
        Year y = Year.now(ZoneId.of("UTC"));

        Month currentMonth = LocalDate.now().getMonth();
        if (Month.JANUARY.equals(currentMonth)) {
            y = y.minusYears(1);
        }

        LocalDate ld = y.atDay(1);
        LocalDateTime time = ld.atTime(0, 0, 0, 0);
        Instant instant = time.toInstant(ZoneOffset.UTC);
        long epochMilli = instant.toEpochMilli();
        return epochMilli;
    }


    /** @return key composed of tstamp and index
     * key stores tstamp in secods, so millisecond part of tstamp is gone
     * during enconding proces.
     **/
    long key(final long tstamp, final long index) {
        if (index > maxIndex) {
            throw new RuntimeException("Index outside allowed range. index: '" + index + "', maxIndex: '" + maxIndex + "'");
        }

        long offsetMs = tstamp - epoch;
        long offsetSec = offsetMs / 1000;
        long hi = offsetSec << 35;
        long lo = index & mask;
        long key = hi | lo;
        return key;
    }

    /** @return tstamp in ms, extracted from key */
    public long tstamp(final long key) {
        return (key >>> 35) * 1000l + epoch;
    }

    /**
     * @return true if tsamps are in same second
     */
    public boolean equalSec(long tstampA, long tstampB) {
        long a = (tstampA / 1000l);
        long b = (tstampB / 1000l);
        return a == b;
    }

    /** @return index */
    long index(final long key) {
        return key & mask;
    }

}
