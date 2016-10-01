<section id="self-service-forms-section">
        <ul id="self-service-forms" class="uk-switcher">

            <!-- CREATE ANOMALY FUNCTIONS -->
            <li id="create-anomaly-functions-tab">
                <!-- PLACEHOLDER OF ANOMALY FUNCTION FORM TEMPLATE -->
            </li>

            <!-- MANAGE EXISTING ANOMALY FUNCTIONS -->
            <li id="manage-existing-anomaly-functions-tab">
                <div  class="title-box full-width">
                    <h2>Manage existing anomaly functions</h2>
                </div>

                <label class="uk-form-label bold-label required">Dataset</label>
                <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false" class="uk-button-group uk-display-inline-block uk-margin-bottom">
                    <div class="selected-dataset uk-button" value="">Select dataset
                    </div>
                    <div class="uk-button uk-button-primary" type="button"><i class="uk-icon-caret-down"></i>
                    </div>
                    <div class="landing-dataset uk-dropdown uk-dropdown-small uk-dropdown-bottom" style="top: 30px; left: 0px;">
                    </div>
                </div>

                <div id="existing-anomaly-functions-table-placeholder"></div>

                <div id="toggle-alert-modal" class="uk-modal">
                    <div class="uk-modal-dialog">
                        <div class="uk-modal-header">
                            <a class="uk-modal-close uk-close position-top-right"></a>
                        </div>
                            <p><b>Turn
                                <span id="turn-on-anomaly-function">on
                                </span>
                                <span id="turn-off-anomaly-function">off
                                </span></b>
                                monitoring and alerts of
                            <b>
                                <span class="function-to-toggle">
                                </span>
                            </b>
                                function?
                            </p>
                        <div class="uk-modal-footer">
                            <div id="toggle-active-state-error" class="uk-alert uk-alert-danger hidden" rel="self-service">
                                <p></p>
                            </div>
                            <div id="toggle-active-state-success" class="uk-alert uk-alert-success hidden" rel="self-service">
                                <p></p>
                            </div>
                            <button type="button" id="confirm-toggle-active-state"  class="uk-button uk-button-primary">Confirm</button>
                            <button type="button" id="close-toggle-alert-modal" class="uk-button uk-modal-close">Close</button>
                        </div>
                    </div>
                </div>
                <!-- DELETE FUNCTION MODAL-->
                <div id="delete-function-modal" class="uk-modal">
                    <div class="uk-modal-dialog">
                        <div class="uk-modal-header">Delete anomaly function <a class="uk-modal-close uk-close position-top-right"></a>
                        </div>

                        <p>Are you sure about deleting <span id="function-to-delete"></span> function?</p>
                        <div class="uk-modal-footer">
                            <div id="delete-anomaly-function-error" class="uk-alert uk-alert-danger hidden" rel="self-service">
                                <p></p>
                            </div>
                            <div id="delete-anomaly-function-success" class="uk-alert uk-alert-success hidden" rel="self-service">
                                <p></p>
                            </div>
                            <button type="button" id="confirm-delete-anomaly-function"  class="uk-button uk-button-primary">Delete</button>
                            <button type="button" id="close-delete-anomaly-function-modal"  class="uk-button uk-modal-close">Close</button>
                        </div>
                    </div>
                </div>
                <!-- UPDATE FUNCTION MODAL-->
                <div id="update-function-modal" class="uk-modal">
                    <div class="uk-modal-dialog uk-modal-dialog-large">
                        <div class="uk-modal-header">
                            <h3>Update anomaly function</h3><a class="uk-modal-close uk-close position-top-right"></a>
                        </div>
                        <div id="update-anomaly-functions-form-placeholder">
                            <!-- PLACEHOLDER OF ANOMALY FUNCTION FORM TEMPLATE -->
                        </div>
                    </div>
                </div>
            </li>
            <li id="manage-alerts">
                <div class="title-box full-width" style="margin-top:15px;">
                    <h2>Manage Alerts</h2>
                </div>
                <table id="configure-emails-form-table" initiallenght="100" class="uk-form">
                    <tr>
                        <td>Dataset
                        </td>
                        <td>
                            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false"
                                 class="uk-button-group">
                                <div id="collection" class="selected-dataset uk-button" value="">Select dataset
                                </div>
                                <div class="uk-button uk-button-primary" type="button"><i
                                        class="uk-icon-caret-down"></i>
                                </div>
                                <div class="landing-dataset uk-dropdown uk-dropdown-small uk-dropdown-bottom"
                                     style="top: 30px; left: 0px;">
                                </div>
                            </div>
                        </td>

                        <td>Metric</td>
                        <td>
                            <div data-uk-dropdown="{mode:'click'}" aria-haspopup="true" aria-expanded="false"
                                 class="uk-button-group">
                                <div id="selected-metric" class="uk-button" data-uk-tooltip title="">Select metric</div>
                                <button class="add-single-metric-btn uk-button uk-button-primary" type="button"><i
                                        class="uk-icon-caret-down"></i></button>
                                <div class="uk-dropdown uk-dropdown-small">
                                    <ul class="metric-list single-metric-list uk-nav uk-nav-dropdown single-select">
                                    </ul>
                                </div>
                            </div>
                        </td>
                        <td></td>
                    </tr>
                    <tr>
                        <td>Alert <br>recipients</td>
                        <td colspan="3">
                            <textarea id='to-address' maxlength="1000" style="width:100%; min-height:90px;"></textarea><input type='hidden' id='email-id' name='emailId' value=''/>
                            <div id="manage-alerts-error" class="uk-alert uk-alert-danger hidden">
                                <p></p>
                            </div>
                            <div id="manage-alerts-success" class="uk-alert uk-alert-success hidden">
                                <p>Success</p>
                            </div>
                        </td>
                        <td>
                            <button id="save-emails" class="uk-button uk-button-primary" type='button'>Update recipients
                            </button>
                        </td>
                    </tr>
                    <tr>
                        <td colspan="5" style="text-align: center;">
                        <div class="multiselect-header">Linked functions</div>
                        <div class="multiselect-header">Unlinked functions</div>

                            <div class="multiselect-content-box"><ul id="linked-functions"></ul></div>
                            <div class="multiselect-content-box"><ul id="unlinked-functions"></ul></div>
                        </td>
                    </tr>
                </table>

            </li>
        </ul>
</section>
