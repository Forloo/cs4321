SELECT * FROM Sailors;
SELECT Sailors.A, Sailors.C FROM Sailors;
SELECT S.A FROM Sailors S;
SELECT * FROM Sailors S WHERE S.A < 3;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT DISTINCT R.G FROM Reserves R;
SELECT * FROM Sailors ORDER BY Sailors.B;
SELECT * FROM Sailors WHERE Sailors.A >= 4;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G AND Sailors.A<=1 AND Sailors.B>=4 and Reserves.G<5