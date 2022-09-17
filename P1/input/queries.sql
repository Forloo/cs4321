SELECT * FROM Sailors;
SELECT Sailors.A FROM Sailors;
SELECT S.A FROM Sailors S;
SELECT * FROM Sailors S WHERE S.A < 3;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G AND Sailors.A <= 1 AND Sailors.B >= 4 and Reserves.G < 5;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = 3 AND Reserves.G = 1;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A <= R.G AND B.E = 2;
SELECT S.B FROM Sailors S, Reserves R, Boats B, Ships H WHERE S.A <= R.G AND B.E = 2 AND H.J < 22;
SELECT * FROM Sailors S1, Sailors S3, Sailors S2 WHERE S2.A = S1.A;
SELECT * FROM Sailors S1, Sailors S3, Sailors S2 WHERE S3.A = 4;
SELECT * FROM Sailors S, Boats B, Reserves R, Ships H, Boats X WHERE R.G >= S.A AND B.E = 2 AND H.J < 22 AND X.E < 2;
SELECT DISTINCT R.G FROM Reserves R;
SELECT * FROM Sailors ORDER BY Sailors.B, Sailors.C;
SELECT DISTINCT S1.A, S2.A FROM Sailors S1, Sailors S2 WHERE S1.A > S2.A;
SELECT B.A FROM Boats B