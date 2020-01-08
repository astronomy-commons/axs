package org.dirac.axs

import org.scalatest.{FlatSpec, OptionValues, Matchers}

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

import FrameFunctions._


class FrameFunctionsTest extends UnitSpec {
  "calcSeparation" should "return correct separations along equator" in {
    val ra_centers = Seq(0.0, 45.0, 90.0, 135.0, 180.0, 225.0, 270.0, 315)
    for (ra <- ra_centers) {
        val separation_deg = FrameFunctions.calcSeparation(ra,  ra + 1.0/3600.0, 0.0, 0.0)
        val separation_arcsec = separation_deg * 3600.0
        separation_arcsec should equal (1.0 +- 0.0001)
    }
  }

  "calcSeparation" should "return correct big separations along equator" in {
    val ra_centers = Seq(0.0, 45.0, 90.0, 135.0, 180.0, 225.0, 270.0, 315)
    for (ra <- ra_centers) {
        val separation_deg = FrameFunctions.calcSeparation(ra, ra + 1.0, 0.0, 0.0)
        separation_deg should equal (1.0 +- 0.0001)
    }
  }

  "calcSeparation" should "return correct separations along latitude" in {
    val dec_centers = Seq(0.0, 20.0, 45.0, 55.0, 75.0, 80.0)
    for (dec <- dec_centers) {
        val separation_deg = FrameFunctions.calcSeparation(0.0, 0.0, dec, dec + 1.0/3600.0)
        val separation_arcsec = separation_deg * 3600.0
        separation_arcsec should equal (1.0 +- 0.0001)
    }
  }

  "calcSeparation" should "return correct cos(dec) separations" in {
    val dec_centers = Seq(0.0, 20.0, 45.0, 55.0, 75.0, 80.0)
    for (dec <- dec_centers) {
        val separation_deg = FrameFunctions.calcSeparation(0.0, 2.0/3600.0 / Math.cos(Math.toRadians(dec)), dec, dec)
        val separation_arcsec = separation_deg * 3600.0
        separation_arcsec should equal (2.0 +- 0.0001)
    }
  }

  "calcSeparation" should "return correct separations across RA=360" in {
    val dec_centers = Seq(0.0, 20.0, 45.0, 55.0, 75.0, 80.0)
    for (dec <- dec_centers) {
        val half_separation = 0.5/3600.0 / Math.cos(Math.toRadians(dec))
        val separation_deg = FrameFunctions.calcSeparation(360 - half_separation, half_separation, dec, dec)
        val separation_arcsec = separation_deg * 3600.0
        separation_arcsec should equal (1.0 +- 0.0001)
    }
  }


}
