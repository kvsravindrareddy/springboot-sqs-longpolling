package com.veera.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import static com.google.common.base.Preconditions.checkNotNull;

import org.yaml.snakeyaml.external.biz.base64Coder.Base64Coder;


public class Utils {
	
	public static String serializeToBase64(Serializable object) throws IOException {
        checkNotNull(object);

        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = null;
        try {
            objectStream = new ObjectOutputStream(byteArrayStream);
            objectStream.writeObject(object);
            return new String(Base64Coder.encode(byteArrayStream.toByteArray()));
        } finally {
            if (objectStream != null)
                objectStream.close();
        }
    }

    public static Object deserializeFromBase64(String data) throws IOException, ClassNotFoundException {
        checkNotNull(data);

        ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(Base64Coder.decode(data));

        ObjectInputStream objectStream = null;
        try {
            objectStream = new ObjectInputStream(byteArrayStream);
            return objectStream.readObject();
        } finally {
            if (objectStream != null)
                objectStream.close();
        }
    }

}
