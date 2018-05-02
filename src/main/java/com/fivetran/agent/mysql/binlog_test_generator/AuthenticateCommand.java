/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.binlog_test_generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The main idea is that no password are sent between client & server on
 * connection and that no password are saved in mysql in a decodable form.
 * On connection a random string is generated and sent to the client (salt).
 * <p>
 * The client generates a new string with a random generator inited with
 * the hash values from the password and the sent string.
 * <p>
 * This 'check' string is sent to the server where it is compared with
 * a string generated from the stored hash_value of the password and the
 * random string.
 */

public class AuthenticateCommand {

    // todo: likely move this along with related classes to another repo once tests no longer need to be generated

    private static int MAX_PACKET_LENGTH = 4;
    private static int MIN_PACKET_LENGTH = 1;

    public static byte[] greetingResponse(String username, String password, int collation, String salt) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] passwordSHA1 = "".equals(password) ? new byte[0] : passwordCompatibleWithMySQL411(password, salt);
        int clientCapabilities = ClientCapabilities.LONG_FLAG | ClientCapabilities.PROTOCOL_41;

        PacketUtil.writeInt(buffer, clientCapabilities, MAX_PACKET_LENGTH);
        PacketUtil.writeInt(buffer, 0, MAX_PACKET_LENGTH);
        PacketUtil.writeInt(buffer, collation, MIN_PACKET_LENGTH);

        for (int i = 0; i < 23; i++)
            buffer.write(0);

        PacketUtil.writeString(buffer, username);
        PacketUtil.writeInt(buffer, passwordSHA1.length, MIN_PACKET_LENGTH);
        
        buffer.write(passwordSHA1);
        return buffer.toByteArray();
    }

    private static byte[] passwordCompatibleWithMySQL411(String password, String salt) {
        try {
            // see mysql/sql/password.c scramble(...) at https://github.com/mysql/mysql-server
            MessageDigest sha = MessageDigest.getInstance("SHA-1");

            byte[] passwordHash = sha.digest(password.getBytes());

            return xor(passwordHash, sha.digest(union(salt.getBytes(), sha.digest(passwordHash))));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] union(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];

        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);

        return r;
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] r = new byte[a.length];

        for (int i = 0; i < r.length; i++)
            r[i] = (byte) (a[i] ^ b[i]);

        return r;
    }
}
