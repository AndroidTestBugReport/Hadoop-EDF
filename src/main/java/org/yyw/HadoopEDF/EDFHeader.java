package org.yyw.HadoopEDF;

/**
 * This class represents the complete header of an EDF-File.
 */
public class EDFHeader
{       //intialize the variables of header information
        String idCode = null;
        String subjectID = null;
        String recordingID = null;
        String startDate = null;
        String startTime = null;
        int bytesInHeader = 0;
        String formatVersion = null;
        int numberOfRecords = 0;
        double durationOfRecords = 0;
        int numberOfChannels = 0;
        String[] channelLabels = null;
        String[] transducerTypes = null;
        String[] dimensions = null;
        Double[] minInUnits = null;
        Double[] maxInUnits = null;
        Integer[] digitalMin = null;
        Integer[] digitalMax = null;
        String[] prefilterings = null;
        Integer[] numberOfSamples = null;
        byte[][] reserveds = null;

        public String getIdCode()
        {
                return idCode;
        }

        public String getSubjectID()
        {
                return subjectID;
        }

        public String getRecordingID()
        {
                return recordingID;
        }

        public String getStartDate()
        {
                return startDate;
        }

        public String getStartTime()
        {
                return startTime;
        }

        public int getBytesInHeader()
        {
                return bytesInHeader;
        }

        public String getFormatVersion()
        {
                return formatVersion;
        }

        public int getNumberOfRecords()
        {
                return numberOfRecords;
        }

        public double getDurationOfRecords()
        {
                return durationOfRecords;
        }

        public int getNumberOfChannels()
        {
                return numberOfChannels;
        }

        public String[] getChannelLabels()
        {
                return channelLabels;
        }

        public String[] getTransducerTypes()
        {
                return transducerTypes;
        }

        public String[] getDimensions()
        {
                return dimensions;
        }

        public Double[] getMinInUnits()
        {
                return minInUnits;
        }

        public Double[] getMaxInUnits()
        {
                return maxInUnits;
        }

        public Integer[] getDigitalMin()
        {
                return digitalMin;
        }

        public Integer[] getDigitalMax()
        {
                return digitalMax;
        }

        public String[] getPrefilterings()
        {
                return prefilterings;
        }

        public Integer[] getNumberOfSamples()
        {
                return numberOfSamples;
        }

        public byte[][] getReserveds()
        {
                return reserveds;
        }

}
