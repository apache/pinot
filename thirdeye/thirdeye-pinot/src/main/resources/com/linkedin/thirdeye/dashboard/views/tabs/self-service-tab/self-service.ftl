<section id="self-service-forms-section">
        <ul id="self-service-forms" class="uk-switcher">

            <!-- CREATE ANOMALY FUNCTIONS -->
            <li id="create-anomaly-functions-tab">
            </li>

            <!-- MANAGE EXISTING ANOMALY FUNCTIONS -->
            <li id="manage-existing-anomaly-functions-tab">
                <div  class="title-box full-width">
                    <h3>Manage existing anomaly functions</h3>
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
                        <div class="uk-modal-header">...</div>
                        <a class="uk-modal-close uk-close"></a>
                        <p>Are you sure about turning off ... function alert?</p>
                        <div class="uk-modal-footer">
                            <button type="button" id=""  class="uk-button">Turn it off</button>
                            <button type="button" id=""  class="uk-button">Keep it active</button>
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
                            <div id="delete-anomaly-function-success" class="uk-alert uk-alert-success hidden" rel="self-service">
                                <p></p>
                            </div>
                            <button type="button" id="confirm-delete-anomaly-function"  class="uk-button uk-button-primary">Delete</button>
                            <button type="button" id="close"  class="uk-button uk-modal-close">Close</button>
                        </div>
                    </div>
                </div>
                <!-- UPDATE FUNCTION MODAL-->
                <div id="update-function-modal" class="uk-modal">
                    <div class="uk-modal-dialog">
                        <div class="uk-modal-header">
                            <h3>Update anomaly function</h3><a class="uk-modal-close uk-close position-top-right"></a>
                        </div>
                        <div id="update-anomaly-functions-form-placeholder"></div>
                    </div>
                </div>
            </li>
        </ul>
</section>
