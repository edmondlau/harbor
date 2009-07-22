//
//  UpdateFunction.java
//  
//
//  Created by Edmond Lau on 1/30/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb;

import java.io.*;

public interface UpdateFunction extends Serializable {

	public Tuple update(Tuple t);

}