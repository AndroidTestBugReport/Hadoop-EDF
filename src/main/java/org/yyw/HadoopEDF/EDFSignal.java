package org.yyw.HadoopEDF;

/**
 * This class represents the complete data records of an EDF-File.
 */
public class EDFSignal
{

        Double[] unitsInDigit;
        Double[] dc;//20180214
        short[][] digitalValues;
        double[][] valuesInUnits;

        public Double[] getUnitsInDigit()
        {
                return unitsInDigit;
        }

        public short[][] getDigitalValues()
        {
                return digitalValues;
        }

        public double[][] getValuesInUnits()
        {
                return valuesInUnits;/*valueInUnits is the final value what we want to see and needs to be calculated by the unitsInDigit and DigitalValues*/
        }
}