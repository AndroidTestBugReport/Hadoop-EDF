package org.yyw.HadoopEDF.ParallelProcessing.SequentialProgram;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;


abstract class ParseUtils
{
        public static String[] readBulkASCIIFromStream(InputStream is, int size, int length) throws IOException
        {
                String[] result = new String[length];
                for (int i = 0; i < length; i++)
                {
                        result[i] = readASCIIFromStream(is, size);
                }
                return result;
        }

        public static Double[] readBulkDoubleFromStream(InputStream is, int size, int length) throws IOException
        {
                Double[] result = new Double[length];
                for (int i = 0; i < length; i++)
                        result[i] = Double.parseDouble(readASCIIFromStream(is, size).trim());
                return result;
        }

        public static Integer[] readBulkIntFromStream(InputStream is, int size, int length) throws IOException
        {
                Integer[] result = new Integer[length];
                for (int i = 0; i < length; i++)
                        result[i] = Integer.parseInt(readASCIIFromStream(is, size).trim());
                return result;
        }

        public static String readASCIIFromStream2(InputStream is, int size) throws IOException
        {
                int len;
                byte[] data = new byte[size];
                len = is.read(data);//inputstream.read(), Reads the next byte of data from the input stream. The value byte is returned as an int in the range 0 to 255 . If no byte is available because the end of the stream has been reached, the value -1 is returned. 
                //byte[] g ={48,49,46, 48, 49,46,56,53};System.out.println(new String(g));���Ϊ01.01.85��Ascii��ֵΪ48ʱ������ʮ���Ƶ�0��Ascii��ֵΪ49ʱ������ʮ���Ƶ�1��
                if (len != data.length)
                        throw new EDFParserException();
                return new String(data, EDFConstants.CHARSET);
                //��������ǰ��ֽ�����תΪ�ַ����õģ���һ���������ֽ����飬�ڶ����������ַ����롣
                //charset��character set�ַ�������
                //byte[] f ={65,66��97, 98, 99,100,101,102};
    			//System.out.println(new String(f));
                //���Ϊ  ABabcdef
                //�ڵ���new String(byte[] b)������췽��ʱ��java����ݴ�������ݰ��յ�ǰ������򴴽�String����������뷽ʽ��ΪGBK�����������������ģ� 
                //����дͼƬ���� 
        }
        //this new function needs to replace the original one readASCIIFromStream2, because it can't be used in the remote environment
        public static String readASCIIFromStream(InputStream is, int size) throws IOException {
        	int len;
        	int pos = 0;
        	byte[] buffer = new byte[1];
        	byte[] data = new byte[size];
        	
        	while(pos < size && (len = is.read(buffer)) != -1) {
        		data[pos] = buffer[0];
        		pos++;
        	}
			return new String(data, EDFConstants.CHARSET);
        	
        }
        public static <T> T[] removeElement(T[] array, int i)
        //ArrayList ��һ��������У��൱�� ��̬���顣
        {
                if (i < 0)
                        return array;
                if (i == 0)
                        return Arrays.copyOfRange(array, 1, array.length);
                T[] result = Arrays.copyOfRange(array, 0, array.length - 1);
                System.arraycopy(array, i + 1, result, i + 1 - 1, array.length - (i + 1));
                return result;
        }
}
