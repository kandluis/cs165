-- Needs test14.dsl to have been executed first.
-- tbl3 has a secondary b-tree tree index on col1 and col2, and a clustered index on col7 with the form of a sorted column
-- testing shared scan on 100 queries
--
-- Query in SQL:
-- Q1: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col4 >= 78091301 and tbl3.col4 < 118548668--
-- Q2: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col6 >= 153472534 and tbl3.col6 < 190489750--
-- Q3: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col3 >= -250003324 and tbl3.col3 < -153542055--
-- Q4: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col1 >= 360161038 and tbl3.col1 < 430400334--
-- Q5: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col2 >= 174964847 and tbl3.col2 < 234227183--
-- Q6: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= -367021856 and tbl3.col5 < -284434020--
-- Q7: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col7 >= 113326821 and tbl3.col7 < 141646368--
-- Q8: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col1 >= 441002272 and tbl3.col1 < 488716814--
-- Q9: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col7 >= -185636195 and tbl3.col7 < -119462531--
-- Q10: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col1 >= 84546226 and tbl3.col1 < 172851319--
-- Q11: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col2 >= -87129792 and tbl3.col2 < -59360427--
-- Q12: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col6 >= 157890939 and tbl3.col6 < 222902219--
-- Q13: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col2 >= 67235477 and tbl3.col2 < 120298588--
-- Q14: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col1 >= 275512701 and tbl3.col1 < 307758138--
-- Q15: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col6 >= 447693925 and tbl3.col6 < 505186288--
-- Q16: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col2 >= -225296608 and tbl3.col2 < -154002650--
-- Q17: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col1 >= -44265141 and tbl3.col1 < -6894806--
-- Q18: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col6 >= 277511390 and tbl3.col6 < 338521394--
-- Q19: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col4 >= -494991426 and tbl3.col4 < -416548436--
-- Q20: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col3 >= -420074968 and tbl3.col3 < -361470774--
-- Q21: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col3 >= 369545546 and tbl3.col3 < 445047037--
-- Q22: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col4 >= 410533818 and tbl3.col4 < 474542045--
-- Q23: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col1 >= -10613883 and tbl3.col1 < 48994212--
-- Q24: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col3 >= 84935049 and tbl3.col3 < 150335078--
-- Q25: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col2 >= -58653183 and tbl3.col2 < 38997682--
-- Q26: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col1 >= -497169574 and tbl3.col1 < -432013190--
-- Q27: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col1 >= -391243588 and tbl3.col1 < -332187713--
-- Q28: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col4 >= -41359716 and tbl3.col4 < 16378595--
-- Q29: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col4 >= 226215201 and tbl3.col4 < 318560817--
-- Q30: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col3 >= 149529118 and tbl3.col3 < 242744663--
-- Q31: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col2 >= 119112000 and tbl3.col2 < 195638496--
-- Q32: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= 329423194 and tbl3.col5 < 390343915--
-- Q33: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= -64643317 and tbl3.col7 < 24770102--
-- Q34: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= -453764114 and tbl3.col7 < -360281715--
-- Q35: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= 481319360 and tbl3.col7 < 527730790--
-- Q36: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col1 >= 95132764 and tbl3.col1 < 189101240--
-- Q37: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col3 >= -420254323 and tbl3.col3 < -380655100--
-- Q38: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col1 >= 499698038 and tbl3.col1 < 591263085--
-- Q39: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col6 >= 195236794 and tbl3.col6 < 259420890--
-- Q40: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col5 >= 55519873 and tbl3.col5 < 100461610--
-- Q41: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col5 >= 127128892 and tbl3.col5 < 167848424--
-- Q42: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col7 >= 371337450 and tbl3.col7 < 445257198--
-- Q43: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col3 >= -410977715 and tbl3.col3 < -381833981--
-- Q44: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col4 >= 259399505 and tbl3.col4 < 331562621--
-- Q45: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col5 >= 6506961 and tbl3.col5 < 69571682--
-- Q46: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col4 >= -158474130 and tbl3.col4 < -78983150--
-- Q47: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col4 >= -22991906 and tbl3.col4 < 38001447--
-- Q48: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col6 >= 301096974 and tbl3.col6 < 356491240--
-- Q49: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= 492349412 and tbl3.col7 < 571398582--
-- Q50: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col6 >= 336456972 and tbl3.col6 < 362680808--
-- Q51: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col3 >= 240442123 and tbl3.col3 < 311215095--
-- Q52: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col2 >= -331469608 and tbl3.col2 < -299310616--
-- Q53: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= -280123662 and tbl3.col7 < -199255210--
-- Q54: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col2 >= 71894452 and tbl3.col2 < 129842903--
-- Q55: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col7 >= -483951887 and tbl3.col7 < -443852342--
-- Q56: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col5 >= 22819317 and tbl3.col5 < 108555336--
-- Q57: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= -173301758 and tbl3.col7 < -144727706--
-- Q58: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col1 >= 65179224 and tbl3.col1 < 154659451--
-- Q59: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col5 >= -328569839 and tbl3.col5 < -291739661--
-- Q60: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col7 >= 499837966 and tbl3.col7 < 527173370--
-- Q61: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col4 >= -150644332 and tbl3.col4 < -95838232--
-- Q62: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col3 >= -354491026 and tbl3.col3 < -264573770--
-- Q63: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= 222559031 and tbl3.col5 < 321510974--
-- Q64: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col6 >= 322304307 and tbl3.col6 < 373055394--
-- Q65: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col4 >= -305663742 and tbl3.col4 < -248196424--
-- Q66: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col7 >= -312536623 and tbl3.col7 < -239995514--
-- Q67: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col7 >= 293598100 and tbl3.col7 < 345612415--
-- Q68: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col1 >= -448162272 and tbl3.col1 < -399839264--
-- Q69: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col1 >= -323256760 and tbl3.col1 < -250542801--
-- Q70: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col2 >= 447384849 and tbl3.col2 < 475592549--
-- Q71: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col4 >= -154662112 and tbl3.col4 < -115042289--
-- Q72: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col3 >= 252345823 and tbl3.col3 < 312462825--
-- Q73: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col7 >= -326893763 and tbl3.col7 < -235919881--
-- Q74: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col3 >= 13355759 and tbl3.col3 < 105103269--
-- Q75: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col1 >= 459738708 and tbl3.col1 < 493617104--
-- Q76: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col6 >= -189161728 and tbl3.col6 < -142414286--
-- Q77: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col7 >= -154417725 and tbl3.col7 < -61376565--
-- Q78: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= 32526875 and tbl3.col5 < 58443401--
-- Q79: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col5 >= 193408990 and tbl3.col5 < 225676112--
-- Q80: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col4 >= 76962210 and tbl3.col4 < 131897012--
-- Q81: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col3 >= 468813389 and tbl3.col3 < 507951011--
-- Q82: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col4 >= -425133621 and tbl3.col4 < -339292028--
-- Q83: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col6 >= -161502296 and tbl3.col6 < -67605868--
-- Q84: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col3 >= 125330981 and tbl3.col3 < 224335358--
-- Q85: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col2 >= -210798428 and tbl3.col2 < -124248746--
-- Q86: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col2 >= 114703242 and tbl3.col2 < 183546410--
-- Q87: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col4 >= -468904197 and tbl3.col4 < -443494567--
-- Q88: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col1 >= 270723639 and tbl3.col1 < 369159775--
-- Q89: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col3 >= -485371660 and tbl3.col3 < -427823180--
-- Q90: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= 401555762 and tbl3.col5 < 467932624--
-- Q91: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col3 >= -40895399 and tbl3.col3 < 9290713--
-- Q92: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col5 >= -11173457 and tbl3.col5 < 67027144--
-- Q93: SELECT tbl3.col3 FROM tbl3 WHERE tbl3.col1 >= -234686440 and tbl3.col1 < -196480879--
-- Q94: SELECT tbl3.col4 FROM tbl3 WHERE tbl3.col2 >= -421423727 and tbl3.col2 < -357056300--
-- Q95: SELECT tbl3.col1 FROM tbl3 WHERE tbl3.col1 >= -379615260 and tbl3.col1 < -301710639--
-- Q96: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col6 >= 75792799 and tbl3.col6 < 148433333--
-- Q97: SELECT tbl3.col6 FROM tbl3 WHERE tbl3.col3 >= 9759189 and tbl3.col3 < 88237605--
-- Q98: SELECT tbl3.col5 FROM tbl3 WHERE tbl3.col5 >= 146309601 and tbl3.col5 < 211477802--
-- Q99: SELECT tbl3.col2 FROM tbl3 WHERE tbl3.col3 >= 191299214 and tbl3.col3 < 247717378--
-- Q100: SELECT tbl3.col7 FROM tbl3 WHERE tbl3.col3 >= 169353766 and tbl3.col3 < 203021296--
s1=select(db1.tbl3.col4,78091301,118548668)
f1=fetch(db1.tbl3.col4,s1)
tuple(f1)
s2=select(db1.tbl3.col6,153472534,190489750)
f2=fetch(db1.tbl3.col6,s2)
tuple(f2)
s3=select(db1.tbl3.col3,-250003324,-153542055)
f3=fetch(db1.tbl3.col4,s3)
tuple(f3)
s4=select(db1.tbl3.col1,360161038,430400334)
f4=fetch(db1.tbl3.col7,s4)
tuple(f4)
s5=select(db1.tbl3.col2,174964847,234227183)
f5=fetch(db1.tbl3.col3,s5)
tuple(f5)
s6=select(db1.tbl3.col5,-367021856,-284434020)
f6=fetch(db1.tbl3.col5,s6)
tuple(f6)
s7=select(db1.tbl3.col7,113326821,141646368)
f7=fetch(db1.tbl3.col7,s7)
tuple(f7)
s8=select(db1.tbl3.col1,441002272,488716814)
f8=fetch(db1.tbl3.col1,s8)
tuple(f8)
s9=select(db1.tbl3.col7,-185636195,-119462531)
f9=fetch(db1.tbl3.col3,s9)
tuple(f9)
s10=select(db1.tbl3.col1,84546226,172851319)
f10=fetch(db1.tbl3.col4,s10)
tuple(f10)
s11=select(db1.tbl3.col2,-87129792,-59360427)
f11=fetch(db1.tbl3.col2,s11)
tuple(f11)
s12=select(db1.tbl3.col6,157890939,222902219)
f12=fetch(db1.tbl3.col4,s12)
tuple(f12)
s13=select(db1.tbl3.col2,67235477,120298588)
f13=fetch(db1.tbl3.col6,s13)
tuple(f13)
s14=select(db1.tbl3.col1,275512701,307758138)
f14=fetch(db1.tbl3.col5,s14)
tuple(f14)
s15=select(db1.tbl3.col6,447693925,505186288)
f15=fetch(db1.tbl3.col2,s15)
tuple(f15)
s16=select(db1.tbl3.col2,-225296608,-154002650)
f16=fetch(db1.tbl3.col3,s16)
tuple(f16)
s17=select(db1.tbl3.col1,-44265141,-6894806)
f17=fetch(db1.tbl3.col2,s17)
tuple(f17)
s18=select(db1.tbl3.col6,277511390,338521394)
f18=fetch(db1.tbl3.col5,s18)
tuple(f18)
s19=select(db1.tbl3.col4,-494991426,-416548436)
f19=fetch(db1.tbl3.col3,s19)
tuple(f19)
s20=select(db1.tbl3.col3,-420074968,-361470774)
f20=fetch(db1.tbl3.col3,s20)
tuple(f20)
s21=select(db1.tbl3.col3,369545546,445047037)
f21=fetch(db1.tbl3.col5,s21)
tuple(f21)
s22=select(db1.tbl3.col4,410533818,474542045)
f22=fetch(db1.tbl3.col2,s22)
tuple(f22)
s23=select(db1.tbl3.col1,-10613883,48994212)
f23=fetch(db1.tbl3.col5,s23)
tuple(f23)
s24=select(db1.tbl3.col3,84935049,150335078)
f24=fetch(db1.tbl3.col3,s24)
tuple(f24)
s25=select(db1.tbl3.col2,-58653183,38997682)
f25=fetch(db1.tbl3.col4,s25)
tuple(f25)
s26=select(db1.tbl3.col1,-497169574,-432013190)
f26=fetch(db1.tbl3.col3,s26)
tuple(f26)
s27=select(db1.tbl3.col1,-391243588,-332187713)
f27=fetch(db1.tbl3.col1,s27)
tuple(f27)
s28=select(db1.tbl3.col4,-41359716,16378595)
f28=fetch(db1.tbl3.col7,s28)
tuple(f28)
s29=select(db1.tbl3.col4,226215201,318560817)
f29=fetch(db1.tbl3.col4,s29)
tuple(f29)
s30=select(db1.tbl3.col3,149529118,242744663)
f30=fetch(db1.tbl3.col5,s30)
tuple(f30)
s31=select(db1.tbl3.col2,119112000,195638496)
f31=fetch(db1.tbl3.col2,s31)
tuple(f31)
s32=select(db1.tbl3.col5,329423194,390343915)
f32=fetch(db1.tbl3.col5,s32)
tuple(f32)
s33=select(db1.tbl3.col7,-64643317,24770102)
f33=fetch(db1.tbl3.col6,s33)
tuple(f33)
s34=select(db1.tbl3.col7,-453764114,-360281715)
f34=fetch(db1.tbl3.col6,s34)
tuple(f34)
s35=select(db1.tbl3.col7,481319360,527730790)
f35=fetch(db1.tbl3.col6,s35)
tuple(f35)
s36=select(db1.tbl3.col1,95132764,189101240)
f36=fetch(db1.tbl3.col5,s36)
tuple(f36)
s37=select(db1.tbl3.col3,-420254323,-380655100)
f37=fetch(db1.tbl3.col2,s37)
tuple(f37)
s38=select(db1.tbl3.col1,499698038,591263085)
f38=fetch(db1.tbl3.col2,s38)
tuple(f38)
s39=select(db1.tbl3.col6,195236794,259420890)
f39=fetch(db1.tbl3.col4,s39)
tuple(f39)
s40=select(db1.tbl3.col5,55519873,100461610)
f40=fetch(db1.tbl3.col3,s40)
tuple(f40)
s41=select(db1.tbl3.col5,127128892,167848424)
f41=fetch(db1.tbl3.col3,s41)
tuple(f41)
s42=select(db1.tbl3.col7,371337450,445257198)
f42=fetch(db1.tbl3.col5,s42)
tuple(f42)
s43=select(db1.tbl3.col3,-410977715,-381833981)
f43=fetch(db1.tbl3.col1,s43)
tuple(f43)
s44=select(db1.tbl3.col4,259399505,331562621)
f44=fetch(db1.tbl3.col2,s44)
tuple(f44)
s45=select(db1.tbl3.col5,6506961,69571682)
f45=fetch(db1.tbl3.col2,s45)
tuple(f45)
s46=select(db1.tbl3.col4,-158474130,-78983150)
f46=fetch(db1.tbl3.col2,s46)
tuple(f46)
s47=select(db1.tbl3.col4,-22991906,38001447)
f47=fetch(db1.tbl3.col4,s47)
tuple(f47)
s48=select(db1.tbl3.col6,301096974,356491240)
f48=fetch(db1.tbl3.col5,s48)
tuple(f48)
s49=select(db1.tbl3.col7,492349412,571398582)
f49=fetch(db1.tbl3.col6,s49)
tuple(f49)
s50=select(db1.tbl3.col6,336456972,362680808)
f50=fetch(db1.tbl3.col1,s50)
tuple(f50)
s51=select(db1.tbl3.col3,240442123,311215095)
f51=fetch(db1.tbl3.col3,s51)
tuple(f51)
s52=select(db1.tbl3.col2,-331469608,-299310616)
f52=fetch(db1.tbl3.col2,s52)
tuple(f52)
s53=select(db1.tbl3.col7,-280123662,-199255210)
f53=fetch(db1.tbl3.col6,s53)
tuple(f53)
s54=select(db1.tbl3.col2,71894452,129842903)
f54=fetch(db1.tbl3.col7,s54)
tuple(f54)
s55=select(db1.tbl3.col7,-483951887,-443852342)
f55=fetch(db1.tbl3.col7,s55)
tuple(f55)
s56=select(db1.tbl3.col5,22819317,108555336)
f56=fetch(db1.tbl3.col6,s56)
tuple(f56)
s57=select(db1.tbl3.col7,-173301758,-144727706)
f57=fetch(db1.tbl3.col6,s57)
tuple(f57)
s58=select(db1.tbl3.col1,65179224,154659451)
f58=fetch(db1.tbl3.col5,s58)
tuple(f58)
s59=select(db1.tbl3.col5,-328569839,-291739661)
f59=fetch(db1.tbl3.col3,s59)
tuple(f59)
s60=select(db1.tbl3.col7,499837966,527173370)
f60=fetch(db1.tbl3.col5,s60)
tuple(f60)
s61=select(db1.tbl3.col4,-150644332,-95838232)
f61=fetch(db1.tbl3.col7,s61)
tuple(f61)
s62=select(db1.tbl3.col3,-354491026,-264573770)
f62=fetch(db1.tbl3.col5,s62)
tuple(f62)
s63=select(db1.tbl3.col5,222559031,321510974)
f63=fetch(db1.tbl3.col5,s63)
tuple(f63)
s64=select(db1.tbl3.col6,322304307,373055394)
f64=fetch(db1.tbl3.col2,s64)
tuple(f64)
s65=select(db1.tbl3.col4,-305663742,-248196424)
f65=fetch(db1.tbl3.col4,s65)
tuple(f65)
s66=select(db1.tbl3.col7,-312536623,-239995514)
f66=fetch(db1.tbl3.col2,s66)
tuple(f66)
s67=select(db1.tbl3.col7,293598100,345612415)
f67=fetch(db1.tbl3.col4,s67)
tuple(f67)
s68=select(db1.tbl3.col1,-448162272,-399839264)
f68=fetch(db1.tbl3.col3,s68)
tuple(f68)
s69=select(db1.tbl3.col1,-323256760,-250542801)
f69=fetch(db1.tbl3.col7,s69)
tuple(f69)
s70=select(db1.tbl3.col2,447384849,475592549)
f70=fetch(db1.tbl3.col3,s70)
tuple(f70)
s71=select(db1.tbl3.col4,-154662112,-115042289)
f71=fetch(db1.tbl3.col5,s71)
tuple(f71)
s72=select(db1.tbl3.col3,252345823,312462825)
f72=fetch(db1.tbl3.col2,s72)
tuple(f72)
s73=select(db1.tbl3.col7,-326893763,-235919881)
f73=fetch(db1.tbl3.col6,s73)
tuple(f73)
s74=select(db1.tbl3.col3,13355759,105103269)
f74=fetch(db1.tbl3.col5,s74)
tuple(f74)
s75=select(db1.tbl3.col1,459738708,493617104)
f75=fetch(db1.tbl3.col3,s75)
tuple(f75)
s76=select(db1.tbl3.col6,-189161728,-142414286)
f76=fetch(db1.tbl3.col1,s76)
tuple(f76)
s77=select(db1.tbl3.col7,-154417725,-61376565)
f77=fetch(db1.tbl3.col5,s77)
tuple(f77)
s78=select(db1.tbl3.col5,32526875,58443401)
f78=fetch(db1.tbl3.col5,s78)
tuple(f78)
s79=select(db1.tbl3.col5,193408990,225676112)
f79=fetch(db1.tbl3.col2,s79)
tuple(f79)
s80=select(db1.tbl3.col4,76962210,131897012)
f80=fetch(db1.tbl3.col5,s80)
tuple(f80)
s81=select(db1.tbl3.col3,468813389,507951011)
f81=fetch(db1.tbl3.col2,s81)
tuple(f81)
s82=select(db1.tbl3.col4,-425133621,-339292028)
f82=fetch(db1.tbl3.col7,s82)
tuple(f82)
s83=select(db1.tbl3.col6,-161502296,-67605868)
f83=fetch(db1.tbl3.col1,s83)
tuple(f83)
s84=select(db1.tbl3.col3,125330981,224335358)
f84=fetch(db1.tbl3.col6,s84)
tuple(f84)
s85=select(db1.tbl3.col2,-210798428,-124248746)
f85=fetch(db1.tbl3.col2,s85)
tuple(f85)
s86=select(db1.tbl3.col2,114703242,183546410)
f86=fetch(db1.tbl3.col2,s86)
tuple(f86)
s87=select(db1.tbl3.col4,-468904197,-443494567)
f87=fetch(db1.tbl3.col3,s87)
tuple(f87)
s88=select(db1.tbl3.col1,270723639,369159775)
f88=fetch(db1.tbl3.col1,s88)
tuple(f88)
s89=select(db1.tbl3.col3,-485371660,-427823180)
f89=fetch(db1.tbl3.col1,s89)
tuple(f89)
s90=select(db1.tbl3.col5,401555762,467932624)
f90=fetch(db1.tbl3.col5,s90)
tuple(f90)
s91=select(db1.tbl3.col3,-40895399,9290713)
f91=fetch(db1.tbl3.col1,s91)
tuple(f91)
s92=select(db1.tbl3.col5,-11173457,67027144)
f92=fetch(db1.tbl3.col1,s92)
tuple(f92)
s93=select(db1.tbl3.col1,-234686440,-196480879)
f93=fetch(db1.tbl3.col3,s93)
tuple(f93)
s94=select(db1.tbl3.col2,-421423727,-357056300)
f94=fetch(db1.tbl3.col4,s94)
tuple(f94)
s95=select(db1.tbl3.col1,-379615260,-301710639)
f95=fetch(db1.tbl3.col1,s95)
tuple(f95)
s96=select(db1.tbl3.col6,75792799,148433333)
f96=fetch(db1.tbl3.col6,s96)
tuple(f96)
s97=select(db1.tbl3.col3,9759189,88237605)
f97=fetch(db1.tbl3.col6,s97)
tuple(f97)
s98=select(db1.tbl3.col5,146309601,211477802)
f98=fetch(db1.tbl3.col5,s98)
tuple(f98)
s99=select(db1.tbl3.col3,191299214,247717378)
f99=fetch(db1.tbl3.col2,s99)
tuple(f99)
s100=select(db1.tbl3.col3,169353766,203021296)
f100=fetch(db1.tbl3.col7,s100)
tuple(f100)
