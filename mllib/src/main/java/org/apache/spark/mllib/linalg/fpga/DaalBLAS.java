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
    String mode = System.getenv("DAAL_MODE");
    if ("0".equals(mode)) {
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useCpu);
    } else if ("1".equals(mode)) {
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useFpgaBalanced);
    } else if ("2".equals(mode)){
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useFpgaMax);
    } else {
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useCpu);
    }

  }

  public double[] dgemm(Batch gemmAlgorithm){
    Result result = gemmAlgorithm.compute();
    HomogenNumericTable cMatrix = (HomogenNumericTable)result.get(ResultId.cMatrix);
    return cMatrix.getDoubleArray();
  }

}
