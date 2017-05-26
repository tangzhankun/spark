/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg.fpga;

import org.zeromq.ZMQ;
import org.apache.spark.mllib.linalg.fpga.Gemm.Matrix;
import org.apache.spark.mllib.linalg.fpga.Gemm.MatrixGemm;
import org.apache.spark.Logging;
import scala.collection.JavaConversions.*;
import java.io.*;
import java.util.List;

public class FpgaBLAS implements Serializable {

    private ZMQ.Context context = ZMQ.context(1);
    private ZMQ.Socket requester = context.socket(ZMQ.DEALER);

    public FpgaBLAS(){
      requester.connect("tcp://localhost:5556");
    }

    /*public ~FpgaBLAS(){
      requester.close();
      context.term();
    }*/

    public List<Double> dgemm(String tAstr, String tBstr, int AnumRows, int BnumCols, int  AnumCols, double alpha, List<Double> Avalues, int lda,List<Double> Bvalues, int ldb, double beta, List<Double> Cvalues, int CnumRows) throws Exception {

        Matrix.Builder matrixA = Matrix.newBuilder();
        matrixA.setDimensionX(AnumRows);
        matrixA.setDimensionY(AnumCols);
        for (int i=0;i<Avalues.size();i++){matrixA.addElement(Avalues.get(i));}

        Matrix.Builder matrixB = Matrix.newBuilder();
        matrixB.setDimensionX(AnumCols);
        matrixB.setDimensionY(BnumCols);
        for (int i=0;i<Bvalues.size();i++){matrixB.addElement(Bvalues.get(i));}

        Matrix.Builder matrixC = Matrix.newBuilder();
        matrixC.setDimensionX(CnumRows);
        matrixC.setDimensionY(BnumCols);
        for (int i=0;i<Cvalues.size();i++){matrixC.addElement(Cvalues.get(i));}

        MatrixGemm.Builder matrixGemm = MatrixGemm.newBuilder();
        matrixGemm.setMatrixA(matrixA.build());
        matrixGemm.setMatrixB(matrixB.build());
        matrixGemm.setMatrixC(matrixC.build());

        //System.out.println("dgemm: Send Request to Server");
        requester.send(matrixGemm.build().toByteArray(), 0);
 
        //System.out.println("dgemm: Wait reply from server");
        byte[] reply = requester.recv(0);
        //System.out.println("dgemm: Get reply from server");
        Matrix matrixReply = Matrix.parseFrom(reply);
        //System.out.println("dgemm: Matrix Result Row number = " + Integer.toString(matrixReply.getDimensionX()));
        //System.out.println("dgemm: Matrix Result Col number = " + Integer.toString(matrixReply.getDimensionY()));
        java.util.List<java.lang.Double> replyValues = matrixReply.getElementList();

        return replyValues;
    }

    public List<Double> dgemv(String tAstr,int AnumRows, int AnumCols, double alpha, List<Double> Avalues, int XnumRows,List<Double> Xvalues, int XnumCols, double beta, List<Double> Yvalues, int YnumCols) throws Exception {

        Matrix.Builder matrixA = Matrix.newBuilder();
        matrixA.setDimensionX(AnumRows);
        matrixA.setDimensionY(AnumCols);
        for (int i=0;i<Avalues.size();i++){matrixA.addElement(Avalues.get(i));}

        Matrix.Builder matrixB = Matrix.newBuilder();
        matrixB.setDimensionX(AnumCols);
        matrixB.setDimensionY(XnumCols);
        for (int i=0;i<Xvalues.size();i++){matrixB.addElement(Xvalues.get(i));}

        Matrix.Builder matrixC = Matrix.newBuilder();
        matrixC.setDimensionX(AnumRows);
        matrixC.setDimensionY(YnumCols);
        for (int i=0;i<Yvalues.size();i++){matrixC.addElement(Yvalues.get(i));}

        MatrixGemm.Builder matrixGemm = MatrixGemm.newBuilder();
        matrixGemm.setMatrixA(matrixA.build());
        matrixGemm.setMatrixB(matrixB.build());
        matrixGemm.setMatrixC(matrixC.build());

        requester.send(matrixGemm.build().toByteArray(), 0);

        byte[] reply = requester.recv(0);
        Matrix matrixReply = Matrix.parseFrom(reply);
        java.util.List<java.lang.Double> replyValues = matrixReply.getElementList();

        return replyValues;
    }
}

