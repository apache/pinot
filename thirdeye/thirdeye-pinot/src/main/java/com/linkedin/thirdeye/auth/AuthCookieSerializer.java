package com.linkedin.thirdeye.auth;

import java.security.Key;
import javax.crypto.Cipher;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class AuthCookieSerializer {
  final Key aesKey;
  final ObjectMapper mapper;

  public AuthCookieSerializer(Key aesKey, ObjectMapper mapper) {
    this.aesKey = aesKey;
    this.mapper = mapper;
  }

  public String serializeCookie(AuthCookie cookie) throws Exception {
    return this.encrypt(this.mapper.writeValueAsString(cookie));
  }

  public AuthCookie deserializeCookie(String serializedCookie) throws Exception {
    return this.mapper.readValue(this.decrypt(serializedCookie), new TypeReference<AuthCookie>() {});
  }

  private String encrypt(String plain) throws Exception {
    // TODO add salt
    Cipher cipher = Cipher.getInstance("AES"); // not threadsafe
    cipher.init(Cipher.ENCRYPT_MODE, aesKey);
    byte[] encrypted = cipher.doFinal(plain.getBytes());
    return Base64.encodeBase64String(encrypted);
  }

  private String decrypt(String crypted) throws Exception {
    Cipher cipher = Cipher.getInstance("AES");
    cipher.init(Cipher.DECRYPT_MODE, aesKey);
    return new String(cipher.doFinal(Base64.decodeBase64(crypted)));
  }
}
