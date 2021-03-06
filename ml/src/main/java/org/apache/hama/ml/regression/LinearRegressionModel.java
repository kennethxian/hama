/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.ml.regression;

import java.math.BigDecimal;

import org.apache.hama.ml.math.DoubleVector;

/**
 * A {@link RegressionModel} for linear regression
 */
public class LinearRegressionModel implements RegressionModel {

  private final CostFunction costFunction;

  public LinearRegressionModel() {
    costFunction = new CostFunction() {
      @Override
      public BigDecimal calculateCostForItem(DoubleVector x, double y, int m,
          DoubleVector theta, HypothesisFunction hypothesis) {
        return BigDecimal.valueOf(y * Math.pow(applyHypothesis(theta, x).doubleValue() - y, 2) / (2 * m));
      }
    };
  }

  @Override
  public BigDecimal applyHypothesis(DoubleVector theta, DoubleVector x) {
    return BigDecimal.valueOf(theta.dotUnsafe(x));
  }

  @Override
  public BigDecimal calculateCostForItem(DoubleVector x, double y, int m,
      DoubleVector theta) {
    return costFunction.calculateCostForItem(x, y, m, theta, this);
  }
}
