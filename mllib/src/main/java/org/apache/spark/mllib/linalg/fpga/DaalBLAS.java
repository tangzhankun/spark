package org.apache.spark.mllib.linalg.fpga;

import com.intel.daal.data_management.data.HomogenNumericTable;
import com.intel.daal.data_management.data.NumericTable;
import com.intel.daal.algorithms.gemm.*;
import com.intel.daal.services.DaalContext;
import com.intel.daal.services.Environment;

import java.sql.Timestamp;
import java.util.List;

public class DaalBLAS {

  //private DaalContext context = new DaalContext();
  //private Batch gemmAlgorithm = new Batch(context, java.lang.Double.class, Method.defaultDense);
  private String mode;
  private int threadCount = 1;
  private String modeName;
  public DaalBLAS(){
    String tc = System.getenv("DAAL_TC");
    if (tc != null && !tc.isEmpty()) {
      this.threadCount = Integer.parseInt(tc);
      Environment.setNumberOfThreads(this.threadCount);
      System.out.println("[DAALgemm] thread count:" + this.threadCount);
    } else {
      Environment.setNumberOfThreads(Environment.getNumberOfThreads());
    }
    this.mode = System.getenv("DAAL_MODE");
    if ("0".equals(mode)) {
      //this.modeName = "useCpu";
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useCpu);
    } else if ("1".equals(mode)) {
      //this.modeName = "useFpgaBalanced";
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useFpgaBalanced);
    } else if ("2".equals(mode)){
      //this.modeName = "useFpgaMax";
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useFpgaMax);
    } else {
      //this.modeName = "useCpu";
      Environment.setAcceleratorMode( Environment.AcceleratorMode.useCpu);
    }

  }

  public double[] dgemm(Batch gemmAlgorithm){
    long threadId = Thread.currentThread().getId();
    long start_timestamp = System.currentTimeMillis();
    Result result = gemmAlgorithm.compute();
    long end_compute_timestamp = System.currentTimeMillis();
    HomogenNumericTable cMatrix = (HomogenNumericTable)result.get(ResultId.cMatrix);
    long end_get_timestamp = System.currentTimeMillis();
    System.out.println("[DAALgemm] TID:" + threadId + ":mode:" + mode +
        ":compute:" + (end_compute_timestamp - start_timestamp) +
        ":getresult:" + (end_get_timestamp - end_compute_timestamp) +
        ":modeName:" + this.modeName
    );
    return cMatrix.getDoubleArray();
  }

}
