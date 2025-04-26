package mt.fireworks;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

public class StringEncodingBench {


    public static void main(String[] args) {
        String text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
                    + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
                    + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris "
                    + "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
                    + "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
                    + "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
                    + "culpa qui officia deserunt mollit anim id est laborum.";

        byte[] byteArray = text.getBytes(UTF_8);

        // Decode the byte array into a CharBuffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        CharBuffer charBuffer = CharBuffer.allocate(2000); // Allocate more than enough space

        CharsetDecoder decoder = UTF_8.newDecoder();
        CoderResult result = decoder.decode(byteBuffer, charBuffer, true);

        if (result.isError()) {
            System.out.println("Decoding error occurred.");
        } else {
            // Flip the charBuffer to prepare it for reading
            charBuffer.flip();

            // Convert CharBuffer to String
            String decodedString = charBuffer.toString();

            // Print the decoded string
            System.out.println(decodedString);
        }

    }

}
