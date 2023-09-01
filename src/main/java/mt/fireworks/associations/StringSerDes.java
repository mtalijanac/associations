package mt.fireworks.associations;

import mt.fireworks.associations.AssociationMap.SerDes;

public class StringSerDes implements SerDes<String> {

    public byte[] marshall(String val) {
        return val.getBytes();
    }

    public String unmarshall(byte[] data) {
        return new String(data);
    }

}
