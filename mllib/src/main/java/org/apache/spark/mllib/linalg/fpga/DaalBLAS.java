package org.apache.spark.mllib.linalg.fpga;

import com.intel.daal.data_management.data.HomogenNumericTable;
import com.intel.daal.data_management.data.NumericTable;
import com.intel.daal.algorithms.gemm.*;
import com.intel.daal.services.DaalContext;
import com.intel.daal.services.Environment;

import java.util.List;

public class DaalBLAS {

  //private DaalContext context = new DaalContext();
  //private Batch gemmAlgorithm = new Batch(context, java.lang.Double.class, Method.defaultDense);

  public DaalBLAS(){
    Environment.setAcceleratorMode( Environment.AcceleratorMode.useFpgaBalanced);
  }

  public double[] dgemm(Batch gemmAlgorithm){
    Result result = gemmAlgorithm.compute();
    HomogenNumericTable cMatrix = (HomogenNumericTable)result.get(ResultId.cMatrix);
    return cMatrix.getDoubleArray();
  }

}
