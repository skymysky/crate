/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.window;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

public class RowNumberWindowFunction implements WindowFunction {

    private static final String NAME = "row_number";

    private final FunctionInfo info;

    private RowNumberWindowFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Object execute(int rowIdx,
                          WindowFrameState currentFrame,
                          List<? extends CollectExpression<Row, ?>> expressions,
                          Input... args) {
        return rowIdx + 1;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    public static void register(WindowFunctionModule module) {
        module.register(NAME, new BaseFunctionResolver(FuncParams.NONE) {

            @Override
            public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                return new RowNumberWindowFunction(
                    new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.INTEGER, FunctionInfo.Type.WINDOW));
            }
        });
    }
}
