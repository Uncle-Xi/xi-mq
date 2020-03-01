package com.ximq.common.util;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Utils {

    private Utils() {}

    // This matches URIs of formats: host:port and protocol:\\host:port
    // IPv6 is supported with [ip] pattern
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    private static final Pattern VALID_HOST_CHARACTERS = Pattern.compile("([0-9a-zA-Z\\-%._:]*)");

    // Prints up to 2 decimal digits. Used for human readable printing
    private static final DecimalFormat TWO_DIGIT_FORMAT = new DecimalFormat("0.##",
        DecimalFormatSymbols.getInstance(Locale.ENGLISH));

    private static final String[] BYTE_SCALE_SUFFIXES = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    public static final String NL = System.getProperty("line.separator");

    
    public static <T extends Comparable<? super T>> List<T> sorted(Collection<T> collection) {
        List<T> res = new ArrayList<>(collection);
        Collections.sort(res);
        return Collections.unmodifiableList(res);
    }

    
    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    
    public static String utf8(ByteBuffer buffer, int length) {
        return utf8(buffer, 0, length);
    }

    
    public static String utf8(ByteBuffer buffer) {
        return utf8(buffer, buffer.remaining());
    }

    
    public static String utf8(ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            return new String(buffer.array(), buffer.arrayOffset() + buffer.position() + offset, length, StandardCharsets.UTF_8);
        } else {
            return utf8(toArray(buffer, offset, length));
        }
    }

    
    public static byte[] utf8(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    
    public static int abs(int n) {
        return (n == Integer.MIN_VALUE) ? 0 : Math.abs(n);
    }

    
    public static long min(long first, long... rest) {
        long min = first;
        for (long r : rest) {
            if (r < min) {
                min = r;
            }
        }
        return min;
    }

    
    public static long max(long first, long... rest) {
        long max = first;
        for (long r : rest) {
            if (r > max)
                max = r;
        }
        return max;
    }


    public static short min(short first, short second) {
        return (short) Math.min(first, second);
    }

    
    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    
    public static byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    
    public static byte[] toArray(ByteBuffer buffer, int size) {
        return toArray(buffer, 0, size);
    }

    
    public static byte[] toNullableArray(ByteBuffer buffer) {
        return buffer == null ? null : toArray(buffer);
    }

    
    public static ByteBuffer wrapNullable(byte[] array) {
        return array == null ? null : ByteBuffer.wrap(array);
    }

    
    public static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    
    public static byte[] copyArray(byte[] src) {
        return Arrays.copyOf(src, src.length);
    }

    
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // this is okay, we just wake up early
            Thread.currentThread().interrupt();
        }
    }

    
    public static <T> T newInstance(Class<T> c) throws Exception {
        return c.getDeclaredConstructor().newInstance();
    }

    
    public static <T> T newInstance(String klass, Class<T> base) throws Exception {
        return Utils.newInstance(loadClass(klass, base));
    }

    
    public static <T> Class<? extends T> loadClass(String klass, Class<T> base) throws ClassNotFoundException {
        return Class.forName(klass, true, Utils.getContextOrKafkaClassLoader()).asSubclass(base);
    }

    
    public static <T> T newParameterizedInstance(String className, Object... params)
            throws ClassNotFoundException {
        Class<?>[] argTypes = new Class<?>[params.length / 2];
        Object[] args = new Object[params.length / 2];
        try {
            Class<?> c = Class.forName(className, true, Utils.getContextOrKafkaClassLoader());
            for (int i = 0; i < params.length / 2; i++) {
                argTypes[i] = (Class<?>) params[2 * i];
                args[i] = params[(2 * i) + 1];
            }
            @SuppressWarnings("unchecked")
            Constructor<T> constructor = (Constructor<T>) c.getConstructor(argTypes);
            return constructor.newInstance(args);
        } catch (NoSuchMethodException e) {
            throw new ClassNotFoundException(String.format("Failed to find " +
                "constructor with %s for %s", Utils.join(argTypes, ", "), className), e);
        } catch (InstantiationException e) {
            throw new ClassNotFoundException(String.format("Failed to instantiate " +
                "%s", className), e);
        } catch (IllegalAccessException e) {
            throw new ClassNotFoundException(String.format("Unable to access " +
                "constructor of %s", className), e);
        } catch (InvocationTargetException e) {
            throw new ClassNotFoundException(String.format("Unable to invoke " +
                "constructor of %s", className), e);
        }
    }

    
    @SuppressWarnings("fallthrough")
    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    
    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    
    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    
    public static boolean validHostPattern(String address) {
        return VALID_HOST_CHARACTERS.matcher(address).matches();
    }

    
    public static String formatAddress(String host, Integer port) {
        return host.contains(":")
                ? "[" + host + "]:" + port // IPv6
                : host + ":" + port;
    }

    
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return String.valueOf(bytes);
        }
        double asDouble = (double) bytes;
        int ordinal = (int) Math.floor(Math.log(asDouble) / Math.log(1024.0));
        double scale = Math.pow(1024.0, ordinal);
        double scaled = asDouble / scale;
        String formatted = TWO_DIGIT_FORMAT.format(scaled);
        try {
            return formatted + " " + BYTE_SCALE_SUFFIXES[ordinal];
        } catch (IndexOutOfBoundsException e) {
            //huge number?
            return String.valueOf(asDouble);
        }
    }

    
    public static <T> String join(T[] strs, String separator) {
        return join(Arrays.asList(strs), separator);
    }

    
    public static <T> String join(Collection<T> list, String separator) {
        Objects.requireNonNull(list);
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = list.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext())
                sb.append(separator);
        }
        return sb.toString();
    }

    
    public static <K, V> String mkString(Map<K, V> map, String begin, String end,
                                         String keyValueSeparator, String elementSeparator) {
        StringBuilder bld = new StringBuilder();
        bld.append(begin);
        String prefix = "";
        for (Map.Entry<K, V> entry : map.entrySet()) {
            bld.append(prefix).append(entry.getKey()).
                    append(keyValueSeparator).append(entry.getValue());
            prefix = elementSeparator;
        }
        bld.append(end);
        return bld.toString();
    }

    
    public static Map<String, String> parseMap(String mapStr, String keyValueSeparator, String elementSeparator) {
        Map<String, String> map = new HashMap<>();

        if (!mapStr.isEmpty()) {
            String[] attrvals = mapStr.split(elementSeparator);
            for (String attrval : attrvals) {
                String[] array = attrval.split(keyValueSeparator, 2);
                map.put(array[0], array[1]);
            }
        }
        return map;
    }

    
    public static Properties loadProps(String filename) throws IOException {
        return loadProps(filename, null);
    }

    
    public static Properties loadProps(String filename, List<String> onlyIncludeKeys) throws IOException {
        Properties props = new Properties();

        if (filename != null) {
            try (InputStream propStream = Files.newInputStream(Paths.get(filename))) {
                props.load(propStream);
            }
        } else {
            System.out.println("Did not load any properties since the property file is not specified");
        }

        if (onlyIncludeKeys == null || onlyIncludeKeys.isEmpty())
            return props;
        Properties requestedProps = new Properties();
        onlyIncludeKeys.forEach(key -> {
            String value = props.getProperty(key);
            if (value != null)
                requestedProps.setProperty(key, value);
        });
        return requestedProps;
    }

    
    public static Map<String, String> propsToStringMap(Properties props) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet())
            result.put(entry.getKey().toString(), entry.getValue().toString());
        return result;
    }

    
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    
    public static byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }

    
    public static byte[] readBytes(ByteBuffer buffer) {
        return Utils.readBytes(buffer, 0, buffer.limit());
    }

    
    public static String readFileAsString(String path) throws IOException {
        try {
            byte[] allBytes = Files.readAllBytes(Paths.get(path));
            return new String(allBytes, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IOException("Unable to read file " + path, ex);
        }
    }

    
    public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
        if (newLength > existingBuffer.capacity()) {
            ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
            existingBuffer.flip();
            newBuffer.put(existingBuffer);
            return newBuffer;
        }
        return existingBuffer;
    }

    
    @SafeVarargs
    public static <T> Set<T> mkSet(T... elems) {
        Set<T> result = new HashSet<>((int) (elems.length / 0.75) + 1);
        for (T elem : elems)
            result.add(elem);
        return result;
    }

    
    public static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
        return new Map.Entry<K, V>() {
            @Override
            public K getKey() {
                return k;
            }

            @Override
            public V getValue() {
                return v;
            }

            @Override
            public V setValue(final V value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    
    @SafeVarargs
    public static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
        final LinkedHashMap<K, V> result = new LinkedHashMap<>();
        for (final Map.Entry<K, V> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    
    public static Properties mkProperties(final Map<String, String> properties) {
        final Properties result = new Properties();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    
    public static void delete(final File file) throws IOException {
        if (file == null)
            return;
        Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFileFailed(Path path, IOException exc) throws IOException {
                // If the root path did not exist, ignore the error; otherwise throw it.
                if (exc instanceof NoSuchFileException && path.toFile().equals(file))
                    return FileVisitResult.TERMINATE;
                throw exc;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException exc) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    
    public static <T> List<T> safe(List<T> other) {
        return other == null ? Collections.emptyList() : other;
    }

   
    public static ClassLoader getKafkaClassLoader() {
        return Utils.class.getClassLoader();
    }

    
    public static ClassLoader getContextOrKafkaClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            return getKafkaClassLoader();
        } else {
            return cl;
        }
    }

    
    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        }
    }

    
    public static void closeAll(Closeable... closeables) throws IOException {
        IOException exception = null;
        for (Closeable closeable : closeables) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (IOException e) {
                if (exception != null)
                    exception.addSuppressed(e);
                else
                    exception = e;
            }
        }
        if (exception != null)
            throw exception;
    }

    
    public static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public static void closeQuietly(AutoCloseable closeable, String name, AtomicReference<Throwable> firstException) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                t.printStackTrace();
            }
        }
    }

    
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    
    public static ByteBuffer sizeDelimited(ByteBuffer buffer, int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    
    public static void readFullyOrFail(FileChannel channel, ByteBuffer destinationBuffer, long position,
                                       String description) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                    "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    
    public static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    
    public static final void readFully(InputStream inputStream, ByteBuffer destinationBuffer) throws IOException {
        if (!destinationBuffer.hasArray()) {
            throw new IllegalArgumentException("destinationBuffer must be backed by an array");
        }
        int initialOffset = destinationBuffer.arrayOffset() + destinationBuffer.position();
        byte[] array = destinationBuffer.array();
        int length = destinationBuffer.remaining();
        int totalBytesRead = 0;
        do {
            int bytesRead = inputStream.read(array, initialOffset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1)
                break;
            totalBytesRead += bytesRead;
        } while (length > totalBytesRead);
        destinationBuffer.position(destinationBuffer.position() + totalBytesRead);
    }

    public static void writeFully(FileChannel channel, ByteBuffer sourceBuffer) throws IOException {
        while (sourceBuffer.hasRemaining())
            channel.write(sourceBuffer);
    }

    
    public static void writeTo(DataOutput out, ByteBuffer buffer, int length) throws IOException {
        if (buffer.hasArray()) {
            out.write(buffer.array(), buffer.position() + buffer.arrayOffset(), length);
        } else {
            int pos = buffer.position();
            for (int i = pos; i < length + pos; i++)
                out.writeByte(buffer.get(i));
        }
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> res = new ArrayList<>();
        while (iterator.hasNext()) {
            res.add(iterator.next());
        }
        return res;
    }

    public static <T> List<T> concatListsUnmodifiable(List<T> left, List<T> right) {
        return concatLists(left, right, Collections::unmodifiableList);
    }

    public static <T> List<T> concatLists(List<T> left, List<T> right, Function<List<T>, List<T>> finisher) {
        return Stream.concat(left.stream(), right.stream())
                .collect(Collectors.collectingAndThen(Collectors.toList(), finisher));
    }

    public static int to32BitField(final Set<Byte> bytes) {
        int value = 0;
        for (final byte b : bytes) {
            value |= 1 << checkRange(b);
        }
        return value;
    }

    private static byte checkRange(final byte i) {
        if (i > 31) {
            throw new IllegalArgumentException("out of range: i>31, i = " + i);
        }
        if (i < 0) {
            throw new IllegalArgumentException("out of range: i<0, i = " + i);
        }
        return i;
    }

    public static Set<Byte> from32BitField(final int intValue) {
        Set<Byte> result = new HashSet<>();
        for (int itr = intValue, count = 0; itr != 0; itr >>>= 1) {
            if ((itr & 1) != 0) {
                result.add((byte) count);
            }
            count++;
        }
        return result;
    }

    public static <K1, V1, K2, V2> Map<K2, V2> transformMap(
            Map<? extends K1, ? extends V1> map,
            Function<K1, K2> keyMapper,
            Function<V1, V2> valueMapper) {
        return map.entrySet().stream().collect(
            Collectors.toMap(
                entry -> keyMapper.apply(entry.getKey()),
                entry -> valueMapper.apply(entry.getValue())
            )
        );
    }
}
