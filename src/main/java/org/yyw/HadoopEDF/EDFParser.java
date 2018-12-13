package org.yyw.HadoopEDF;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.yyw.HadoopEDF.EDFConstants.*;

/**
 * This is an EDFParser which is capable of parsing files in the formats EDF and
 * EDF+.
 *
 * For information about EDF or EDF+ see http://www.edfplus.info/
 */
public class EDFParser
{
        /**
         * Parse the InputStream which should be at the start of an EDF-File. The
         * method returns an object containing the complete content of the EDF-File.
         *
         * @param is
         *            the InputStream to the EDF-File
         * @return the parsed result
         * @throws EDFParserException
         *             if there is an error during parsing
         */
        public static EDFParserResult parseEDF(InputStream is) throws EDFParserException
        {
                EDFParserResult result = parseHeader(is);
                parseSignal(is, result);

                return result;
        }

        /**
         * Parse the InputStream which should be at the start of an EDF-File. The
         * method returns an object containing the complete header of the EDF-File
         *
         * @param is
         *            the InputStream to the EDF-File
         * @return the parsed result
         * @throws EDFParserException
         *             if there is an error during parsing
         */
        public static EDFParserResult parseHeader(InputStream is) throws EDFParserException
        {
                try
                {
                        EDFHeader header = new EDFHeader();
                        EDFParserResult result = new EDFParserResult();
                        result.header = header;

                        header.idCode = ParseUtils.readASCIIFromStream(is, IDENTIFICATION_CODE_SIZE);
                        System.out.println(header.idCode);
                        if (!header.idCode.trim().equals("0")) {
                                throw new EDFParserException();
                        }//���е�idrecord should be zero, otherwise the exception will be throwed
                        header.subjectID = ParseUtils.readASCIIFromStream(is, LOCAL_SUBJECT_IDENTIFICATION_SIZE);
                        header.recordingID = ParseUtils.readASCIIFromStream(is, LOCAL_REOCRDING_IDENTIFICATION_SIZE);
                        header.startDate = ParseUtils.readASCIIFromStream(is, START_DATE_SIZE);
                        header.startTime = ParseUtils.readASCIIFromStream(is, START_TIME_SIZE);
                        header.bytesInHeader = Integer.parseInt(ParseUtils.readASCIIFromStream(is, HEADER_SIZE).trim());/*�ַ��е�trim����������ȥ���ַ���β��Ŀո�ģ�Integer.parseInt����*/
                        header.formatVersion = ParseUtils.readASCIIFromStream(is, DATA_FORMAT_VERSION_SIZE);
                        header.numberOfRecords = Integer.parseInt(
                                ParseUtils.readASCIIFromStream(is, NUMBER_OF_DATA_RECORDS_SIZE).trim());//Integer.parseInt, The method generally is used to convert String to Integer
                        header.durationOfRecords = Double.parseDouble(
                                ParseUtils.readASCIIFromStream(is, DURATION_DATA_RECORDS_SIZE).trim());
                        header.numberOfChannels = Integer.parseInt(
                                ParseUtils.readASCIIFromStream(is, NUMBER_OF_CHANELS_SIZE).trim());

                        parseChannelInformation(is, result);

                        return result;
                } catch (IOException e)
                {
                        throw new EDFParserException(e);
                       
                }
        }

        /**
         * Parse only data EDF file. This method should be invoked only after
         * parseHeader method.
         * It will be populated in result parameter.
         *
         * @param is
         *            stream with EDF file.
         * @param result
         *            results from {parseHeader(is) parseHeader} method
         * @throws EDFParserException
         *             throws if parser don't recognized EDF (EDF+) format in
         *             stream.
         */
        private static void parseSignal(InputStream is, EDFParserResult result) throws EDFParserException
        {
                try
                {
                        EDFSignal signal = new EDFSignal();
                        EDFHeader header = result.getHeader();

                        signal.unitsInDigit = new Double[header.numberOfChannels];//��ʼ������unitsIndigitΪ14
                        
                        signal.dc = new Double[header.numberOfChannels];//dc��������EDFsignalΪ�������ʹ��ǰ����г�ʼ����2018/02/14
                        for (int i = 0; i < signal.unitsInDigit.length; i++){//unitsInDigit is intialized as the number of channels, should be 14, so i=[0:14]
                        	//System.out.println(signal.unitsInDigit.length);
                                signal.unitsInDigit[i] = (header.maxInUnits[i] - header.minInUnits[i])
                                                         / (header.digitalMax[i] - header.digitalMin[i]);//unitsInDigit is saclefac same as in matlab, actually it unitsInDigit is scale of offset or value of offset.
                       	signal.dc[i]=header.maxInUnits[i] - signal.unitsInDigit[i] * header.digitalMax[i];//according to the above equation, we also know signal.dc[i]=header.minInUnits[i] - signal.unitsInDigit[i] * header.digitalMin[i]
                                }//2018/02/14
                      
                        signal.digitalValues = new short[header.numberOfChannels][];//��ʼ������digitalValuesΪ��ά���飬[1][],[2][],[3][]...[14][],���ִ��ڼ����ŵ�
                        signal.valuesInUnits = new double[header.numberOfChannels][];//��ʼ������valuesInUnitsΪ��ά����
                        
                        for (int i = 0; i < header.numberOfChannels; i++)//ѭ���ӵ�һ���ŵ���ʼ
                        {
                                signal.digitalValues[i] = new short[header.numberOfRecords * header.numberOfSamples[i]];//��digitalvalues��ʼ���������ռ�Ϊ��such as digitalvalues[1]=10800*1, digitalvalues[2]=10800*125..
                                signal.valuesInUnits[i] = new double[header.numberOfRecords * header.numberOfSamples[i]];//ͬ��
                        }

                        int samplesPerRecord = 0;//�Ա���samplesPerrecord���г�ʼ��
                        for (int nos : header.numberOfSamples)//�����ŵ�1��ֵΪ1����the number of Records of channel 1 should be 10800, ���ŵ���ֵΪ125ʱ����ÿһ�μ�¼��record������125�Σ�����record[1]=[1...125], record[2]=[1...125],...record[10800]=[1..125],һ�������ŵ�2������Ϊ125*10800 
                        {
                                samplesPerRecord += nos;
                        }

//                        ReadableByteChannel ch = Channels.newChannel(is); // ReadableByteChannel is obtained by Channels. newChannel(in) by passing Input Stream. 
//                        ByteBuffer bytebuf = ByteBuffer.allocate(samplesPerRecord * 2);//bytebuffer���ֽڻ������ȸ�Ҫ�Ž�ȥ����ݷ���ռ�
//                        bytebuf.order(ByteOrder.LITTLE_ENDIAN);//Little-endian format: the sequence addresses/sends/stores the least significant byte first (lowest address) and the most significant byte last (highest address). Most computer systems prefer a single format for all its data;

             
                        
                        for (int i = 0; i < header.numberOfRecords; i++)
                        {       
//                                bytebuf.rewind();//You can use rewind( ) to go back and reread the data in a buffer that has already been flipped. As you can see: it doesn't change the limit. Sometimes, after read data from a buffer, you may want to read it again, rewind() is the right choice for you.
//                         
//                                ch.read(bytebuf);// read() places read bytes at the buffer's position so the position should always be properly set before calling read() This method sets the position to 0
//                                bytebuf.rewind();// The read() method also moves the position so in order to read the new bytes, the buffer's position must be set back to 0
//                               
                             	int len;
                            	int pos = 0;
                            	byte[] buffer = new byte[1];
                            	int size = samplesPerRecord*2;
                            	byte[] data = new byte[size];
                            	while(pos < size && (len = is.read(buffer)) != -1) {
                            		data[pos] = buffer[0];
                            		pos++;
                            	}
                                short[] sa = new short[samplesPerRecord];
                                ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(sa);
                                int m=0;
                            	for (int j = 0; j < header.numberOfChannels; j++)
                                        for (int k = 0; k < header.numberOfSamples[j]; k++)
                                        {
                                                int s = header.numberOfSamples[j] * i + k;
                                                signal.digitalValues[j][s] = sa[m];// getShort(byte[] bArray, short bOff) Concatenates two bytes in a byte array to form a short value,byte has 8 bits, however has 16 bits
                                                signal.valuesInUnits[j][s] =  signal.digitalValues[j][s] * signal.unitsInDigit[j]+signal.dc[j];//this equation is refered by program of matlab
                                                m++;
                                                //System.out.println(signal.valuesInUnits[j][s]);
                                        }
                        }

                        result.annotations = parseAnnotation(header, signal);

                        result.signal = signal;
                } catch (IOException e)
                {
                        throw new EDFParserException(e);
                }
        }

        private static List<EDFAnnotation> parseAnnotation(EDFHeader header, EDFSignal signal)
        {

                if (!header.formatVersion.startsWith("EDF+"))
                        return null;

                int annotationIndex = -1;
                for (int i = 0; i < header.numberOfChannels; i++)
                {
                        if ("EDF Annotations".equals(header.channelLabels[i].trim()))
                        {
                                annotationIndex = i;
                                break;
                        }
                }
                if (annotationIndex == -1)
                        return null;

                short[] s = signal.digitalValues[annotationIndex];
                byte[] b = new byte[s.length * 2];
                for (int i = 0; i < s.length * 2; i += 2)
                {
                        b[i] = (byte) (s[i / 2] % 256);
                        b[i + 1] = (byte) (s[i / 2] / 256 % 256);
                }

                removeAnnotationSignal(header, signal, annotationIndex);

                return parseAnnotations(b);

        }

        private static List<EDFAnnotation> parseAnnotations(byte[] b)
        {
                List<EDFAnnotation> annotations = new ArrayList<>();
                int onSetIndex = 0;
                int durationIndex = -1;
                int annotationIndex = -2;
                int endIndex = -3;
                for (int i = 0; i < b.length - 1; i++)
                {
                        if (b[i] == 21)
                        {
                                durationIndex = i;
                                continue;
                        }
                        if (b[i] == 20 && onSetIndex > annotationIndex)
                        {
                                annotationIndex = i;
                                continue;
                        }
                        if (b[i] == 20 && b[i + 1] == 0)
                        {
                                endIndex = i;
                                continue;
                        }
                        if (b[i] != 0 && onSetIndex < endIndex)
                        {

                                String onSet;
                                String duration;
                                if (durationIndex > onSetIndex)
                                {
                                        onSet = new String(b, onSetIndex, durationIndex - onSetIndex);
                                        duration = new String(b, durationIndex, annotationIndex - durationIndex);
                                } else
                                {
                                        onSet = new String(b, onSetIndex, annotationIndex - onSetIndex);
                                        duration = "";
                                }
                                String annotation = new String(b, annotationIndex, endIndex - annotationIndex);
                                annotations.add(new EDFAnnotation(onSet, duration, annotation.split("[\u0014]")));
                                onSetIndex = i;
                        }
                }
                return annotations;
        }

        private static void removeAnnotationSignal(EDFHeader header, EDFSignal signal, int annotationIndex)
        {
                header.numberOfChannels--;
                ParseUtils.removeElement(header.channelLabels, annotationIndex);
                ParseUtils.removeElement(header.transducerTypes, annotationIndex);
                ParseUtils.removeElement(header.dimensions, annotationIndex);
                ParseUtils.removeElement(header.minInUnits, annotationIndex);
                ParseUtils.removeElement(header.maxInUnits, annotationIndex);
                ParseUtils.removeElement(header.digitalMin, annotationIndex);
                ParseUtils.removeElement(header.digitalMax, annotationIndex);
                ParseUtils.removeElement(header.prefilterings, annotationIndex);
                ParseUtils.removeElement(header.numberOfSamples, annotationIndex);
                ParseUtils.removeElement(header.reserveds, annotationIndex);

                ParseUtils.removeElement(signal.digitalValues, annotationIndex);
                ParseUtils.removeElement(signal.unitsInDigit, annotationIndex);
                ParseUtils.removeElement(signal.valuesInUnits, annotationIndex);
        }

        private static void parseChannelInformation(InputStream is, EDFParserResult result) throws EDFParserException
        {
                try
                {
                        EDFHeader header = result.getHeader();
                        int numberOfChannels = header.numberOfChannels;
                        header.channelLabels = ParseUtils
                                .readBulkASCIIFromStream(is, LABEL_OF_CHANNEL_SIZE, numberOfChannels);
                        header.transducerTypes = ParseUtils
                                .readBulkASCIIFromStream(is, TRANSDUCER_TYPE_SIZE, numberOfChannels);
                        header.dimensions = ParseUtils
                                .readBulkASCIIFromStream(is, PHYSICAL_DIMENSION_OF_CHANNEL_SIZE, numberOfChannels);
                        header.minInUnits = ParseUtils
                                .readBulkDoubleFromStream(is, PHYSICAL_MIN_IN_UNITS_SIZE, numberOfChannels);
                        header.maxInUnits = ParseUtils
                                .readBulkDoubleFromStream(is, PHYSICAL_MAX_IN_UNITS_SIZE, numberOfChannels);
                        header.digitalMin = ParseUtils.readBulkIntFromStream(is, DIGITAL_MIN_SIZE, numberOfChannels);
                        header.digitalMax = ParseUtils.readBulkIntFromStream(is, DIGITAL_MAX_SIZE, numberOfChannels);
                        header.prefilterings = ParseUtils
                                .readBulkASCIIFromStream(is, PREFILTERING_SIZE, numberOfChannels);
                        header.numberOfSamples = ParseUtils
                                .readBulkIntFromStream(is, NUMBER_OF_SAMPLES_SIZE, numberOfChannels);
                        header.reserveds = new byte[numberOfChannels][];
                        for (int i = 0; i < header.reserveds.length; i++)
                        {
                                header.reserveds[i] = new byte[RESERVED_SIZE];
                                is.read(header.reserveds[i]);
                        }
                } catch (IOException e)
                {
                        throw new EDFParserException(e);
                }
        }
}
